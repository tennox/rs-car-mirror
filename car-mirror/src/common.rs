use crate::{
    buffered::BufferedBlockStore,
    cache::Cache,
    dag_walk::DagWalk,
    error::Error,
    incremental_verification::{BlockState, IncrementalDagVerification},
    messages::{PullRequest, PushResponse},
};
use bytes::{Bytes, BytesMut};
use deterministic_bloom::runtime_size::BloomFilter;
use futures::{StreamExt, TryStreamExt};
use iroh_car::{CarHeader, CarReader, CarWriter};
use libipld::{Ipld, IpldCodec};
use libipld_core::{cid::Cid, codec::References};
use std::{collections::HashSet, io::Cursor};
use wnfs_common::{
    utils::{boxed_stream, BoxStream, CondSend},
    BlockStore,
};

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Configuration values (such as byte limits) for the CAR mirror protocol
#[derive(Clone, Debug)]
pub struct Config {
    /// The maximum number of bytes per request that a recipient should accept.
    ///
    /// This only has an effect in non-streaming versions of this protocol.
    /// In streaming versions, car-mirror will check the validity of each block
    /// while streaming.
    ///
    /// By default this is 2MB.
    pub receive_maximum: usize,
    /// The maximum number of bytes per block.
    ///
    /// As long as we can't verify the hash value of a block, we can't verify if we've
    /// been given the data we actuall want or not, thus we need to put a maximum value
    /// on the byte size that we accept per block.
    ///
    /// By default this is 1MB.
    ///
    /// 1MB is also the default maximum block size in IPFS's bitswap protocol.
    /// 256KiB is the default maximum block size that Kubo produces by default when generating
    /// UnixFS blocks.
    ///
    /// `iroh-car` internally has a maximum 4MB limit on a CAR file frame (CID + block), so
    /// any value above 4MB doesn't work.
    pub max_block_size: usize,
    /// The maximum number of roots per request that will be requested by the recipient
    /// to be sent by the sender.
    ///
    /// By default this is 1_000.
    pub max_roots_per_round: usize,
    /// The target false positive rate for the bloom filter that the recipient sends.
    ///
    /// By default it's set to `|num| min(0.001, 0.1 / num)`.
    ///
    /// This default means bloom filters will aim to have a false positive probability
    /// one order of magnitude under the number of elements. E.g. for 100_000 elements,
    /// a false positive probability of 1 in 1 million.
    pub bloom_fpr: fn(u64) -> f64,
    /// Roots of subgraphs the receiving side does not need transferred —
    /// either because it already holds them *completely* (application
    /// invariant, e.g. wovin snapshot roots, which are only ever
    /// recorded/pinned as complete DAGs) or because it deliberately doesn't
    /// want them (shallow/bounded sync). Reason-agnostic by design.
    ///
    /// Effects when set on the block-receiving side:
    /// - Local incremental verification stops at these CIDs instead of
    ///   walking the entire history below them (cost proportional to new
    ///   data, not the full DAG).
    /// - They are sent to the block-sending side as
    ///   [`skip_subgraph_roots`](crate::messages::PullRequest::skip_subgraph_roots)
    ///   on every round's message, so the sender prunes its walk below them
    ///   (old senders ignore the field).
    ///
    /// This is per-transfer state more than static configuration — clone the
    /// base config and fill this in per request. Empty by default (no effect).
    pub skip_subgraph_roots: HashSet<Cid>,
    /// Assume that when the receiver's bloom filter contains a CID, the
    /// receiver has that block's *entire subgraph*, and prune the send-side
    /// traversal below it (instead of walking every block of shared history
    /// just to skip each one individually).
    ///
    /// This is NOT generally true in car-mirror (a bloom describes a flat set
    /// of blocks), but holds for append-only snapshot-chain DAGs like wovin's,
    /// where a receiver only records a snapshot root once it has its full DAG.
    ///
    /// Self-healing: a bloom false positive can prune too much, but the
    /// receiver then explicitly requests the missing subgraph roots in the next
    /// round, and explicitly requested roots always bypass the bloom.
    /// Off by default.
    pub bloom_implies_complete_subgraphs: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            receive_maximum: 2_000_000, // 2 MB
            max_block_size: 1_000_000,  // 1 MB
            max_roots_per_round: 1000,  // max. ~41KB of CIDs
            bloom_fpr: |num_of_elems| f64::min(0.001, 0.1 / num_of_elems as f64),
            skip_subgraph_roots: HashSet::new(),
            bloom_implies_complete_subgraphs: false,
        }
    }
}

/// Some information that the block receiving end provides the block sending end
/// in order to deduplicate block transfers.
#[derive(Clone)]
pub struct ReceiverState {
    /// At least *some* of the subgraph roots that are missing for sure on the receiving end.
    pub missing_subgraph_roots: Vec<Cid>,
    /// An optional bloom filter of all CIDs below the root that the receiving end has.
    pub have_cids_bloom: Option<BloomFilter>,
    /// Roots whose entire subgraphs the sender should neither walk nor send
    /// (the receiver has them completely, or doesn't want them). Explicitly
    /// requested `missing_subgraph_roots` always take precedence. See
    /// [`PullRequest::skip_subgraph_roots`].
    pub skip_subgraph_roots: Vec<Cid>,
}

/// Newtype around bytes that are supposed to represent a CAR file
#[derive(Debug, Clone)]
pub struct CarFile {
    /// The car file contents as bytes.
    /// (`CarFile` is cheap to clone, since `Bytes` is like an `Arc` wrapper around a byte buffer.)
    pub bytes: Bytes,
}

/// A stream of blocks. This requires the underlying futures to be `Send`, except when the target is `wasm32`.
pub type BlockStream<'a> = BoxStream<'a, Result<(Cid, Bytes), Error>>;

/// A stream of byte chunks of a CAR file.
/// The underlying futures are `Send`, except when the target is `wasm32`.
pub type CarStream<'a> = BoxStream<'a, Result<Bytes, Error>>;

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

/// This function is run on the block sending side of the protocol.
///
/// It's used on the client during the push protocol, or on the server
/// during the pull protocol.
///
/// It returns a `CarFile` of (a subset) of all blocks below `root`, that
/// are thought to be missing on the receiving end.
#[tracing::instrument(skip_all, fields(root, last_state))]
pub async fn block_send(
    root: Cid,
    last_state: Option<ReceiverState>,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<CarFile, Error> {
    let bytes = block_send_car_stream(
        root,
        last_state,
        Vec::new(),
        Some(config.receive_maximum),
        config.bloom_implies_complete_subgraphs,
        store,
        cache,
    )
    .await?;

    Ok(CarFile {
        bytes: bytes.into(),
    })
}

/// This is the streaming equivalent of `block_send`.
///
/// It uses the car file format for framing blocks & CIDs in the given `AsyncWrite`.
#[tracing::instrument(skip_all, fields(root, last_state))]
pub async fn block_send_car_stream<W: tokio::io::AsyncWrite + Unpin + Send>(
    root: Cid,
    last_state: Option<ReceiverState>,
    writer: W,
    send_limit: Option<usize>,
    prune_bloom_subgraphs: bool,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<W, Error> {
    let mut block_stream =
        block_send_block_stream_pruning(root, last_state, prune_bloom_subgraphs, store, cache)
            .await?;
    write_blocks_into_car(writer, &mut block_stream, send_limit).await
}

/// This is the car mirror block sending function, but unlike `block_send_car_stream`
/// it leaves framing blocks to the caller.
pub async fn block_send_block_stream<'a>(
    root: Cid,
    last_state: Option<ReceiverState>,
    store: impl BlockStore + 'a,
    cache: impl Cache + 'a,
) -> Result<BlockStream<'a>, Error> {
    block_send_block_stream_pruning(root, last_state, false, store, cache).await
}

/// Like [`block_send_block_stream`], but with optional subgraph pruning below
/// bloom hits (see [`Config::bloom_implies_complete_subgraphs`]).
pub async fn block_send_block_stream_pruning<'a>(
    root: Cid,
    last_state: Option<ReceiverState>,
    prune_bloom_subgraphs: bool,
    store: impl BlockStore + 'a,
    cache: impl Cache + 'a,
) -> Result<BlockStream<'a>, Error> {
    let ReceiverState {
        missing_subgraph_roots,
        have_cids_bloom,
        skip_subgraph_roots,
    } = last_state.unwrap_or(ReceiverState {
        missing_subgraph_roots: vec![root],
        have_cids_bloom: None,
        skip_subgraph_roots: Vec::new(),
    });

    let bloom = handle_missing_bloom(have_cids_bloom);
    let skip_roots: HashSet<Cid> = skip_subgraph_roots.into_iter().collect();

    // Verify that all missing subgraph roots are in the relevant DAG.
    //
    // Validation is deliberately decoupled from pruning: it runs on UNPRUNED
    // reachability (via the references cache — no block bytes needed when
    // warm), while bloom-skips and skip-root pruning apply only to the send
    // stream below. Validating on the pruned walk is what caused the historic
    // pull stall: from round 2 the root itself is a bloom hit, pruning there
    // cut reachability, and every deeper requested root got dropped as
    // "DAG-unrelated" — the server then served 0 blocks forever.
    //
    // Short-circuit: if the only root requested is the DAG root itself,
    // it's trivially valid — skip the expensive full-DAG walk.
    let subgraph_roots = if missing_subgraph_roots == [root] {
        missing_subgraph_roots
    } else {
        verify_missing_subgraph_roots(root, &missing_subgraph_roots, &store, &cache).await?
    };

    let stream = stream_blocks_from_roots(
        subgraph_roots,
        bloom,
        skip_roots,
        prune_bloom_subgraphs,
        store,
        cache,
    );

    Ok(Box::pin(stream))
}

/// This function is run on the block receiving end of the protocol.
///
/// It's used on the client during the pull protocol and on the server
/// during the push protocol.
///
/// It takes a `CarFile`, verifies that its contents are related to the
/// `root` and returns some information to help the block sending side
/// figure out what blocks to send next.
#[tracing::instrument(skip_all, fields(root, car_bytes = last_car.as_ref().map(|car| car.bytes.len())))]
pub async fn block_receive(
    root: Cid,
    last_car: Option<CarFile>,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<ReceiverState, Error> {
    let mut receiver_state = match last_car {
        Some(car) => {
            if car.bytes.len() > config.receive_maximum {
                return Err(Error::TooManyBytes {
                    receive_maximum: config.receive_maximum,
                    bytes_read: car.bytes.len(),
                });
            }

            block_receive_car_stream(root, Cursor::new(car.bytes), config, store, cache).await?
        }
        None => IncrementalDagVerification::new_with_boundary(
            [root],
            config.skip_subgraph_roots.clone(),
            &store,
            &cache,
        )
        .await?
        .into_receiver_state(config.bloom_fpr),
    };

    receiver_state
        .missing_subgraph_roots
        .truncate(config.max_roots_per_round);
    attach_skip_roots(&mut receiver_state, config);

    Ok(receiver_state)
}

/// Like `block_receive`, but allows consuming the CAR file as a stream.
#[tracing::instrument(skip_all, fields(root))]
pub async fn block_receive_car_stream<R: tokio::io::AsyncRead + Unpin + CondSend>(
    root: Cid,
    reader: R,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<ReceiverState, Error> {
    let reader = CarReader::new(reader).await?;

    let mut stream: BlockStream<'_> = Box::pin(
        reader
            .stream()
            .map_ok(|(cid, bytes)| (cid, Bytes::from(bytes)))
            .map_err(Error::CarFileError),
    );

    block_receive_block_stream(root, &mut stream, config, store, cache).await
}

/// Consumes a stream of blocks, verifying their integrity and
/// making sure all blocks are part of the DAG.
pub async fn block_receive_block_stream(
    root: Cid,
    stream: &mut BlockStream<'_>,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<ReceiverState, Error> {
    let max_block_size = config.max_block_size;
    let mut dag_verification = IncrementalDagVerification::new_with_boundary(
        [root],
        config.skip_subgraph_roots.clone(),
        &store,
        &cache,
    )
    .await?;

    while let Some((cid, block)) = stream.try_next().await? {
        let block_bytes = block.len();
        // TODO(matheus23): Find a way to restrict size *before* framing. Possibly inside `CarReader`?
        // Possibly needs making `MAX_ALLOC` in `iroh-car` configurable.
        if block_bytes > config.max_block_size {
            return Err(Error::BlockSizeExceeded {
                cid,
                block_bytes,
                max_block_size,
            });
        }

        match read_and_verify_block(&mut dag_verification, (cid, block), &store, &cache).await? {
            BlockState::Have => {
                // A single already-known CID does NOT imply the rest of the stream is
                // redundant — `bloom_implies_complete_subgraphs` defaults to `false`
                // precisely because a bloom hit only describes one flat block, not a
                // complete subtree (see `Config` docs). Breaking here used to discard
                // every remaining (already-downloaded!) block in the response, forcing
                // near-total re-transfers on stores that share blocks across DAGs (e.g.
                // a global content-addressed store deduping structural blocks across
                // many unrelated roots) — one incidental hit early in the stream could
                // throw away 100K+ blocks that were still wanted. Skip the duplicate and
                // keep consuming; the receiver state built at the end of the stream
                // already reflects everything actually seen.
                tracing::trace!(%cid, "Received block we already have, skipping it");
            }
            BlockState::Unexpected => {
                // We received a block out-of-order. This is weird, but can
                // happen due to bloom filter false positives.
                // Essentially, the sender could've skipped a block that was
                // important for us to verify that further blocks are connected
                // to the root.
                // We should update the endpoint about the skipped block.
                tracing::debug!(%cid, "Received block out of order, stopping transfer");
                break;
            }
            BlockState::Want => {
                // Perfect, we're just getting what we want. Let's continue!
            }
        }
    }

    // No full DAG walk needed here — verify_and_store_block incrementally
    // updates want_cids/have_cids by extracting direct links from each block.

    let mut receiver_state = dag_verification.into_receiver_state(config.bloom_fpr);
    attach_skip_roots(&mut receiver_state, config);
    Ok(receiver_state)
}

/// Like [`block_receive`], but returns verified blocks instead of storing them.
///
/// The returned `Vec<(Cid, Bytes)>` contains all blocks that were verified
/// during this round, in the order they were received. The caller is
/// responsible for persisting them (e.g., batch-importing as a CAR file).
///
/// The `store` parameter is only read from to determine which blocks are
/// already present — no writes are made to it.
#[tracing::instrument(skip_all, fields(root, car_bytes = last_car.as_ref().map(|car| car.bytes.len())))]
pub async fn block_receive_with_blocks(
    root: Cid,
    last_car: Option<CarFile>,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<(ReceiverState, Vec<(Cid, Bytes)>), Error> {
    let buffered = BufferedBlockStore::new(&store);
    let mut receiver_state = match last_car {
        Some(car) => {
            if car.bytes.len() > config.receive_maximum {
                return Err(Error::TooManyBytes {
                    receive_maximum: config.receive_maximum,
                    bytes_read: car.bytes.len(),
                });
            }

            block_receive_car_stream(root, Cursor::new(car.bytes), config, &buffered, &cache)
                .await?
        }
        None => IncrementalDagVerification::new_with_boundary(
            [root],
            config.skip_subgraph_roots.clone(),
            &buffered,
            &cache,
        )
        .await?
        .into_receiver_state(config.bloom_fpr),
    };

    receiver_state
        .missing_subgraph_roots
        .truncate(config.max_roots_per_round);
    attach_skip_roots(&mut receiver_state, config);

    let blocks = buffered.into_blocks();
    Ok((receiver_state, blocks))
}

/// Like [`block_receive_car_stream`], but returns verified blocks instead of storing them.
///
/// See [`block_receive_with_blocks`] for details.
#[tracing::instrument(skip_all, fields(root))]
pub async fn block_receive_car_stream_with_blocks<R: tokio::io::AsyncRead + Unpin + CondSend>(
    root: Cid,
    reader: R,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<(ReceiverState, Vec<(Cid, Bytes)>), Error> {
    let buffered = BufferedBlockStore::new(&store);
    let reader = CarReader::new(reader).await?;

    let mut stream: BlockStream<'_> = Box::pin(
        reader
            .stream()
            .map_ok(|(cid, bytes)| (cid, Bytes::from(bytes)))
            .map_err(Error::CarFileError),
    );

    let state = block_receive_block_stream(root, &mut stream, config, &buffered, &cache).await?;
    let blocks = buffered.into_blocks();
    Ok((state, blocks))
}

/// Turns a stream of blocks (tuples of CIDs and Bytes) into a stream
/// of frames for a CAR file.
///
/// Simply concatenated together, all these frames form a CARv1 file.
///
/// The frame boundaries are after the header section and between each block.
///
/// The first frame will always be a CAR file header frame.
pub async fn stream_car_frames(mut blocks: BlockStream<'_>) -> Result<CarStream<'_>, Error> {
    // https://github.com/wnfs-wg/car-mirror-spec/issues/6
    // CAR files *must* have at least one CID in them, and all of them
    // need to appear as a block in the payload.
    // It would probably make most sense to just write all subgraph roots into this,
    // but we don't know how many of the subgraph roots fit into this round yet,
    // so we're simply writing the first one in here, since we know
    // at least one block will be written (and it'll be that one).
    let Some((cid, block)) = blocks.try_next().await? else {
        tracing::debug!("No blocks to write.");
        return Ok(boxed_stream(futures::stream::empty()));
    };

    let mut writer = CarWriter::new(CarHeader::new_v1(vec![cid]), Vec::new());
    writer.write_header().await?;
    let first_frame = car_frame_from_block((cid, block)).await?;

    let header = writer.finish().await?;
    Ok(boxed_stream(
        futures::stream::iter(vec![Ok(header.into()), Ok(first_frame)])
            .chain(blocks.and_then(car_frame_from_block)),
    ))
}

/// Target size for coalescing CAR block frames before yielding them downstream.
///
/// One `Bytes` per block turns into one transport write / TLS record per block;
/// batching frames up to this many bytes amortizes that overhead across a whole
/// chunk. 64 KiB is large enough to make the per-write cost negligible yet small
/// enough that time-to-first-byte and memory stay low.
const CAR_STREAM_CHUNK_SIZE: usize = 64 * 1024;

/// Like [`stream_car_frames`], but stops once the emitted block frames would
/// exceed `size_limit` bytes — the streaming equivalent of the per-round
/// `receive_maximum` budget that [`write_blocks_into_car`] enforces.
///
/// The first block is always emitted (a CAR must contain at least its root
/// block, mirroring [`write_blocks_into_car`]); subsequent blocks are included
/// only while the running frame total stays under the limit. `None` streams the
/// whole block stream with no cap.
pub async fn stream_car_frames_limited(
    mut blocks: BlockStream<'_>,
    size_limit: Option<usize>,
) -> Result<CarStream<'_>, Error> {
    let Some((cid, block)) = blocks.try_next().await? else {
        tracing::debug!("No blocks to write.");
        return Ok(boxed_stream(futures::stream::empty()));
    };

    let mut writer = CarWriter::new(CarHeader::new_v1(vec![cid]), Vec::new());
    writer.write_header().await?;
    let first_frame = car_frame_from_block((cid, block)).await?;
    let header = writer.finish().await?;

    let stream = async_stream::try_stream! {
        // Coalesce many small block frames into ~CAR_STREAM_CHUNK_SIZE chunks
        // before yielding. Emitting one `Bytes` per block means one downstream
        // write / TLS record per block (Rocket `ByteStream` → hyper); for a
        // large pull that's tens of thousands of sub-KiB writes whose per-write
        // overhead dominates once the block bytes are served from cache. A CAR
        // is a header followed by concatenated frames, so chunk boundaries are
        // invisible to the reader — only the concatenation matters.
        //
        // `emitted_bytes` still counts *frame* bytes (not chunk boundaries) so
        // the `receive_maximum` budget is enforced exactly as before.
        let mut emitted_bytes = header.len() + first_frame.len();
        let mut buf = BytesMut::with_capacity(CAR_STREAM_CHUNK_SIZE);
        buf.extend_from_slice(&header);
        buf.extend_from_slice(&first_frame);

        while let Some((cid, block)) = blocks.try_next().await? {
            let frame = car_frame_from_block((cid, block)).await?;
            if let Some(limit) = size_limit {
                if emitted_bytes + frame.len() > limit {
                    tracing::debug!(%cid, limit, emitted_bytes, frame_len = frame.len(), "stopping CAR stream at receive limit");
                    break;
                }
            }
            emitted_bytes += frame.len();
            buf.extend_from_slice(&frame);
            if buf.len() >= CAR_STREAM_CHUNK_SIZE {
                // Flush the full chunk and start a fresh buffer; TTFB is
                // unaffected in practice (a 64 KiB chunk fills within a few
                // hundred blocks of the walk).
                let chunk = std::mem::replace(&mut buf, BytesMut::with_capacity(CAR_STREAM_CHUNK_SIZE));
                yield chunk.freeze();
            }
        }

        // Flush the trailing partial chunk (also covers the size-limit break).
        if !buf.is_empty() {
            yield buf.freeze();
        }
    };
    Ok(boxed_stream(stream))
}

/// Find all CIDs that a block references.
///
/// This will error out if
/// - the codec is not supported
/// - the block can't be parsed.
pub fn references<E: Extend<Cid>>(
    cid: Cid,
    block: impl AsRef<[u8]>,
    mut refs: E,
) -> Result<E, anyhow::Error> {
    let codec: IpldCodec = cid
        .codec()
        .try_into()
        .map_err(|_| Error::UnsupportedCodec { cid })?;

    <Ipld as References<IpldCodec>>::references(codec, &mut Cursor::new(block), &mut refs)?;
    Ok(refs)
}

//--------------------------------------------------------------------------------------------------
// Private
//--------------------------------------------------------------------------------------------------

async fn car_frame_from_block(block: (Cid, Bytes)) -> Result<Bytes, Error> {
    // TODO(matheus23): I wish this were exposed in iroh-car somehow
    // Instead of having to allocate so many things.

    // The writer will always first emit a header.
    // If we don't force it here, it'll do so in `writer.write()`.
    // We do it here so we find out how many bytes we need to skip.
    let bogus_header = CarHeader::new_v1(vec![Cid::default()]);
    let mut writer = CarWriter::new(bogus_header, Vec::new());
    let start = writer.write_header().await?;

    writer.write(block.0, block.1).await?;
    let mut bytes = writer.finish().await?;

    // This removes the bogus header bytes
    bytes.drain(0..start);

    Ok(bytes.into())
}

/// Ensure that any requested subgraph roots are actually part
/// of the DAG from the root.
///
/// Walks UNPRUNED reachability on purpose (see the call site in
/// [`block_send_block_stream_pruning`]): pruning during validation is what
/// made deeper requested roots unreachable and stalled transfers. Exits early
/// once every requested root has been found, so on the common path (requested
/// roots near the frontier of what the receiver has) this touches only the
/// top of the DAG, served from the references cache.
async fn verify_missing_subgraph_roots(
    root: Cid,
    missing_subgraph_roots: &[Cid],
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<Vec<Cid>, Error> {
    let missing_set: HashSet<Cid> = missing_subgraph_roots.iter().cloned().collect();
    let mut subgraph_roots: Vec<Cid> = Vec::new();
    let mut dag_walk = DagWalk::breadth_first([root]);

    while let Some(item) = dag_walk.next(store, cache).await? {
        let cid = item.to_cid()?;

        if missing_set.contains(&cid) {
            subgraph_roots.push(cid);
            if subgraph_roots.len() == missing_set.len() {
                break;
            }
        }
    }

    if subgraph_roots.len() != missing_subgraph_roots.len() {
        let subgraph_set: HashSet<&Cid> = subgraph_roots.iter().collect();
        let unrelated_roots = missing_subgraph_roots
            .iter()
            .filter(|cid| !subgraph_set.contains(cid))
            .map(|cid| cid.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        tracing::warn!(
            unrelated_roots = %unrelated_roots,
            "got asked for DAG-unrelated blocks"
        );
    }

    Ok(subgraph_roots)
}

/// Attach the receiver's configured skip roots to the outgoing state, so the
/// (stateless) sender learns them on every round. Sorted for deterministic
/// wire bytes.
fn attach_skip_roots(receiver_state: &mut ReceiverState, config: &Config) {
    let mut skips: Vec<Cid> = config.skip_subgraph_roots.iter().cloned().collect();
    skips.sort();
    receiver_state.skip_subgraph_roots = skips;
}

fn handle_missing_bloom(have_cids_bloom: Option<BloomFilter>) -> BloomFilter {
    if let Some(bloom) = &have_cids_bloom {
        tracing::debug!(
            size_bits = bloom.as_bytes().len() * 8,
            hash_count = bloom.hash_count(),
            ones_count = bloom.count_ones(),
            estimated_fpr = bloom.current_false_positive_rate(),
            "received 'have cids' bloom",
        );
    }

    have_cids_bloom.unwrap_or_else(|| BloomFilter::new_with(1, Box::new([0]))) // An empty bloom that contains nothing
}

/// How many block reads to keep in flight while materializing one BFS level.
///
/// Every block in a level is independent — by the time a level is emitted the
/// receiver already wants all of it (each was discovered from its parent in the
/// previous level), so within-level reads can overlap freely. This bounds the
/// in-flight reads (fd / memory / blocking-pool pressure) while letting the
/// per-read syscall+threadpool cost of a disk-backed store (e.g. kubo flatfs,
/// where a single serialized `tokio::fs::read` per block is ~200µs of overhead
/// even when the data is page-cache-hot) overlap instead of summing.
const LEVEL_READ_CONCURRENCY: usize = 64;

/// Stream the blocks to send by walking the DAG **breadth-first, one level at a
/// time**, reading each level's blocks concurrently.
///
/// For the default (`prune_bloom_subgraphs == false`) path this is
/// behaviourally equivalent to a plain breadth-first `DagWalk` — same set of
/// blocks, same bloom-skip / subgraph-root semantics, same parent-before-child
/// (topological) emission order the receiver requires — but instead of one
/// serialized `get_block().await` per block it reads a whole level with bounded
/// concurrency. That's what turns a cold, disk-bound pull from "sum of per-block
/// read latencies" into "level count × one parallel read batch".
///
/// Two intentional, self-healing divergences from the old walk (both in the
/// more-lenient direction, neither reachable on the production path):
///   - Under `prune_bloom_subgraphs == true`, a block reachable *both* through a
///     pruned bloom-hit parent and through a live parent (a shared cross-edge
///     leaf) may be emitted here where the old frontier-pruning dropped it. The
///     receiver already has it under the complete-subgraph invariant, so it
///     responds `Have` and the round converges — no stall.
///   - A bloom-*skipped* block that is missing from the store no longer errors
///     as `CIDNotFound` (the old walk error'd before the skip check). This only
///     arises with a dangling reference in an inconsistent store; we simply
///     don't read a block we weren't going to send. Blocks we *do* send still
///     error as `CIDNotFound` when absent (see below).
///
/// The one read that a block needs serves two purposes at once: its bytes (for
/// blocks we actually send) and its links (to form the next level). Crucially,
/// a bloom-skipped block whose references are already cached costs **zero
/// reads** — it expands purely from the references cache — so warm incremental
/// pulls (where almost every walked block is skipped) still read only the few
/// blocks they send, exactly as before.
fn stream_blocks_from_roots<'a>(
    subgraph_roots: Vec<Cid>,
    bloom: BloomFilter,
    skip_roots: HashSet<Cid>,
    prune_bloom_subgraphs: bool,
    store: impl BlockStore + 'a,
    cache: impl Cache + 'a,
) -> BlockStream<'a> {
    let subgraph_roots_set: HashSet<Cid> = subgraph_roots.iter().cloned().collect();
    // Raw blocks carry no links; like `Cache::references` we never read or cache
    // them for reference extraction.
    let raw_codec: u64 = IpldCodec::Raw.into();
    Box::pin(async_stream::try_stream! {
        let mut visited: HashSet<Cid> = HashSet::new();
        let mut level: Vec<Cid> = Vec::new();
        for cid in subgraph_roots {
            if visited.insert(cid) {
                level.push(cid);
            }
        }

        while !level.is_empty() {
            let mut next_level: Vec<Cid> = Vec::new();

            // Pass 1 — classify the level without reading any blocks. A block
            // needs a read only if we must send it, or if we must walk through
            // it but its links aren't cached. Skipped blocks with cached links
            // (the warm incremental case) expand here for free.
            let mut to_read: Vec<(Cid, bool)> = Vec::new(); // (cid, emit?)
            for &cid in &level {
                // Explicit skip root: the receiver declared it needs nothing
                // below this CID (has it completely, or doesn't want it) —
                // prune unconditionally: don't send, don't descend. Unlike a
                // bloom hit this is an exact, receiver-asserted claim, so it
                // needs no `prune_bloom_subgraphs` opt-in. Explicitly
                // requested roots always win over a skip claim.
                if skip_roots.contains(&cid) && !subgraph_roots_set.contains(&cid) {
                    continue;
                }
                let skipped = should_block_be_skipped(&cid, &bloom, &subgraph_roots_set);
                if !skipped {
                    // Must read to send; links come from the bytes (or cache) in pass 2.
                    to_read.push((cid, true));
                    continue;
                }
                // Skipped (receiver has it). With the complete-subgraph
                // invariant we prune here and never descend; a raw block has no
                // subgraph to descend into either. Otherwise walk through it —
                // from cached links if we can, else read for links.
                if prune_bloom_subgraphs || cid.codec() == raw_codec {
                    continue;
                }
                match cache
                    .get_references_cache(cid)
                    .await
                    .map_err(Error::BlockStoreError)?
                {
                    Some(refs) => {
                        for r in refs {
                            if visited.insert(r) {
                                next_level.push(r);
                            }
                        }
                    }
                    None => to_read.push((cid, false)),
                }
            }
            level.clear();

            // Pass 2 — read the needed blocks with bounded concurrency
            // (`buffered` preserves order → deterministic BFS emission). Each
            // read block expands the frontier (from cached links or by parsing
            // the bytes we just read, populating the cache) and is emitted if we
            // were sending it. A block we need to send but that's absent surfaces
            // as `CIDNotFound` here (as the plain walk's `Missing` → `to_cid()?`
            // did); only bloom-skipped absent blocks are tolerated (see above).
            let mut reads = futures::stream::iter(to_read.into_iter())
                .map(|(cid, emit)| {
                    let store = &store;
                    async move {
                        let bytes =
                            store.get_block(&cid).await.map_err(Error::BlockStoreError)?;
                        Ok::<_, Error>((cid, bytes, emit))
                    }
                })
                .buffered(LEVEL_READ_CONCURRENCY);

            while let Some((cid, bytes, emit)) = reads.try_next().await? {
                if cid.codec() != raw_codec {
                    let refs = match cache
                        .get_references_cache(cid)
                        .await
                        .map_err(Error::BlockStoreError)?
                    {
                        Some(refs) => refs,
                        None => {
                            let refs = references(cid, &bytes, Vec::new())
                                .map_err(Error::ParsingError)?;
                            cache
                                .put_references_cache(cid, refs.clone())
                                .await
                                .map_err(Error::BlockStoreError)?;
                            refs
                        }
                    };
                    for r in refs {
                        if visited.insert(r) {
                            next_level.push(r);
                        }
                    }
                }
                if emit {
                    yield (cid, bytes);
                }
            }

            level = next_level;
        }
    })
}

async fn write_blocks_into_car<W: tokio::io::AsyncWrite + Unpin + Send>(
    write: W,
    blocks: &mut BlockStream<'_>,
    size_limit: Option<usize>,
) -> Result<W, Error> {
    let mut block_bytes = 0;

    // https://github.com/wnfs-wg/car-mirror-spec/issues/6
    // CAR files *must* have at least one CID in them, and all of them
    // need to appear as a block in the payload.
    // It would probably make most sense to just write all subgraph roots into this,
    // but we don't know how many of the subgraph roots fit into this round yet,
    // so we're simply writing the first one in here, since we know
    // at least one block will be written (and it'll be that one).
    let Some((cid, block)) = blocks.try_next().await? else {
        tracing::debug!("No blocks to write.");
        return Ok(write);
    };

    let mut writer = CarWriter::new(CarHeader::new_v1(vec![cid]), write);

    block_bytes += writer.write(cid, block).await?;

    while let Some((cid, block)) = blocks.try_next().await? {
        tracing::debug!(
            cid = %cid,
            num_bytes = block.len(),
            "writing block to CAR",
        );

        // Let's be conservative, assume a 64-byte CID (usually ~40 byte)
        // and a 4-byte frame size varint (3 byte would be enough for an 8MiB frame).
        let added_bytes = 64 + 4 + block.len();

        if let Some(receive_limit) = size_limit {
            if block_bytes + added_bytes > receive_limit {
                tracing::debug!(%cid, receive_limit, block_bytes, added_bytes, "Skipping block because it would go over the receive limit");
                break;
            }
        }

        block_bytes += writer.write(cid, &block).await?;
    }

    Ok(writer.finish().await?)
}

fn should_block_be_skipped(cid: &Cid, bloom: &BloomFilter, subgraph_roots: &HashSet<Cid>) -> bool {
    bloom.contains(&cid.to_bytes()) && !subgraph_roots.contains(cid)
}

/// Takes a block and stores it iff it's one of the blocks we're currently trying to retrieve.
/// Returns the block state of the received block.
async fn read_and_verify_block(
    dag_verification: &mut IncrementalDagVerification,
    (cid, block): (Cid, Bytes),
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<BlockState, Error> {
    match dag_verification.block_state(cid) {
        BlockState::Have => Ok(BlockState::Have),
        BlockState::Unexpected => {
            tracing::trace!(
                cid = %cid,
                "received block out of order (possibly due to bloom false positive)"
            );
            Ok(BlockState::Unexpected)
        }
        BlockState::Want => {
            dag_verification
                .verify_and_store_block((cid, block), store, cache)
                .await?;
            Ok(BlockState::Want)
        }
    }
}

//--------------------------------------------------------------------------------------------------
// Implementations
//--------------------------------------------------------------------------------------------------

impl From<PushResponse> for ReceiverState {
    fn from(push: PushResponse) -> Self {
        let PushResponse {
            subgraph_roots,
            bloom_hash_count: hash_count,
            bloom_bytes: bytes,
            skip_subgraph_roots,
        } = push;

        Self {
            missing_subgraph_roots: subgraph_roots,
            have_cids_bloom: Self::bloom_deserialize(hash_count, bytes),
            skip_subgraph_roots,
        }
    }
}

impl From<PullRequest> for ReceiverState {
    fn from(pull: PullRequest) -> Self {
        let PullRequest {
            resources,
            bloom_hash_count: hash_count,
            bloom_bytes: bytes,
            skip_subgraph_roots,
        } = pull;

        Self {
            missing_subgraph_roots: resources,
            have_cids_bloom: Self::bloom_deserialize(hash_count, bytes),
            skip_subgraph_roots,
        }
    }
}

impl From<ReceiverState> for PushResponse {
    fn from(receiver_state: ReceiverState) -> PushResponse {
        let ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom,
            skip_subgraph_roots,
        } = receiver_state;

        let (hash_count, bytes) = ReceiverState::bloom_serialize(have_cids_bloom);

        PushResponse {
            subgraph_roots: missing_subgraph_roots,
            bloom_hash_count: hash_count,
            bloom_bytes: bytes,
            skip_subgraph_roots,
        }
    }
}

impl From<ReceiverState> for PullRequest {
    fn from(receiver_state: ReceiverState) -> PullRequest {
        let ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom,
            skip_subgraph_roots,
        } = receiver_state;

        let (hash_count, bytes) = ReceiverState::bloom_serialize(have_cids_bloom);

        PullRequest {
            resources: missing_subgraph_roots,
            bloom_hash_count: hash_count,
            bloom_bytes: bytes,
            skip_subgraph_roots,
        }
    }
}

impl ReceiverState {
    fn bloom_serialize(bloom: Option<BloomFilter>) -> (u32, Vec<u8>) {
        match bloom {
            Some(bloom) => (bloom.hash_count() as u32, bloom.as_bytes().to_vec()),
            None => (3, Vec::new()),
        }
    }

    fn bloom_deserialize(hash_count: u32, bytes: Vec<u8>) -> Option<BloomFilter> {
        if bytes.is_empty() {
            None
        } else {
            Some(BloomFilter::new_with(
                hash_count as usize,
                bytes.into_boxed_slice(),
            ))
        }
    }
}

impl std::fmt::Debug for ReceiverState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let have_cids_bloom = self
            .have_cids_bloom
            .as_ref()
            .map_or("None".into(), |bloom| {
                format!(
                    "Some(BloomFilter(k_hashes = {}, {} bytes))",
                    bloom.hash_count(),
                    bloom.as_bytes().len()
                )
            });
        f.debug_struct("ReceiverState")
            .field(
                "missing_subgraph_roots.len() == ",
                &self.missing_subgraph_roots.len(),
            )
            .field("have_cids_bloom", &have_cids_bloom)
            .field(
                "skip_subgraph_roots.len() == ",
                &self.skip_subgraph_roots.len(),
            )
            .finish()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{cache::NoCache, test_utils::assert_cond_send_sync};
    use assert_matches::assert_matches;
    use testresult::TestResult;
    use wnfs_common::{MemoryBlockStore, CODEC_RAW};

    #[allow(clippy::unreachable, unused)]
    fn test_assert_send() {
        assert_cond_send_sync(|| {
            block_send(
                unimplemented!(),
                unimplemented!(),
                unimplemented!(),
                unimplemented!() as MemoryBlockStore,
                NoCache,
            )
        });
        assert_cond_send_sync(|| {
            block_receive(
                unimplemented!(),
                unimplemented!(),
                unimplemented!(),
                unimplemented!() as &MemoryBlockStore,
                &NoCache,
            )
        })
    }

    #[test]
    fn test_receiver_state_is_not_a_huge_debug() -> TestResult {
        let state = ReceiverState {
            have_cids_bloom: Some(BloomFilter::new_from_size(4096, 1000)),
            missing_subgraph_roots: vec![Cid::default(); 1000],
            skip_subgraph_roots: vec![Cid::default(); 1000],
        };

        let debug_print = format!("{state:#?}");

        assert!(debug_print.len() < 1000);

        Ok(())
    }

    #[test_log::test(async_std::test)]
    async fn test_stream_car_frame_empty() -> TestResult {
        let car_frames = stream_car_frames(futures::stream::empty().boxed()).await?;
        let frames: Vec<Bytes> = car_frames.try_collect().await?;

        assert!(frames.is_empty());

        Ok(())
    }

    #[test_log::test(async_std::test)]
    async fn test_write_blocks_into_car_empty() -> TestResult {
        let car_file =
            write_blocks_into_car(Vec::new(), &mut futures::stream::empty().boxed(), None).await?;

        assert!(car_file.is_empty());

        Ok(())
    }

    #[test_log::test(async_std::test)]
    async fn test_block_receive_block_stream_block_size_exceeded() -> TestResult {
        let store = &MemoryBlockStore::new();

        let block_small: Bytes = b"This one is small".to_vec().into();
        let block_big: Bytes = b"This one is very very very big".to_vec().into();
        let root_small = store.put_block(block_small.clone(), CODEC_RAW).await?;
        let root_big = store.put_block(block_big.clone(), CODEC_RAW).await?;

        let config = &Config {
            max_block_size: 20,
            ..Config::default()
        };

        block_receive_block_stream(
            root_small,
            &mut futures::stream::iter(vec![Ok((root_small, block_small))]).boxed(),
            config,
            MemoryBlockStore::new(),
            NoCache,
        )
        .await?;

        let result = block_receive_block_stream(
            root_small,
            &mut futures::stream::iter(vec![Ok((root_big, block_big))]).boxed(),
            config,
            MemoryBlockStore::new(),
            NoCache,
        )
        .await;

        assert_matches!(result, Err(Error::BlockSizeExceeded { .. }));

        Ok(())
    }
}

#[cfg(test)]
mod wovin_chain_tests {
    //! Tests for the wovin-style DAG optimizations:
    //! append-only chains of snapshots (root → applog chunk → many small
    //! leaves, root → prev snapshot root), where a receiver that has a
    //! snapshot root always has its complete subgraph.

    use super::*;
    use crate::{cache::NoCache, pull, push};
    use libipld::{cbor::DagCborCodec, Ipld};
    use std::collections::BTreeMap;
    use testresult::TestResult;
    use wnfs_common::{encode, MemoryBlockStore};

    /// Store one wovin-style snapshot: root → chunk → `leaf_count` leaves,
    /// plus `prev` link on the root if given. Content is deterministic per
    /// `tag`, so building the same snapshot into two stores yields equal CIDs.
    /// Returns (root, number of blocks in this snapshot).
    async fn put_snapshot(
        store: &impl BlockStore,
        prev: Option<Cid>,
        leaf_count: usize,
        tag: &str,
    ) -> anyhow::Result<(Cid, usize)> {
        let mut leaves = Vec::new();
        for i in 0..leaf_count {
            let cid = store
                .put_block(
                    encode(&Ipld::String(format!("leaf-{tag}-{i}")), DagCborCodec)?,
                    DagCborCodec.into(),
                )
                .await?;
            leaves.push(Ipld::Link(cid));
        }
        let chunk_cid = store
            .put_block(
                encode(&Ipld::List(leaves), DagCborCodec)?,
                DagCborCodec.into(),
            )
            .await?;
        let mut map = BTreeMap::new();
        map.insert("applogs".to_string(), Ipld::Link(chunk_cid));
        if let Some(prev) = prev {
            map.insert("prev".to_string(), Ipld::Link(prev));
        }
        let root = store
            .put_block(encode(&Ipld::Map(map), DagCborCodec)?, DagCborCodec.into())
            .await?;
        Ok((root, leaf_count + 2))
    }

    async fn count_car_blocks(car: &CarFile) -> anyhow::Result<usize> {
        let reader = CarReader::new(Cursor::new(car.bytes.clone())).await?;
        let mut count = 0;
        let mut stream = Box::pin(reader.stream());
        while stream.try_next().await?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    async fn assert_complete_dag(root: Cid, store: &impl BlockStore) -> TestResult {
        DagWalk::breadth_first([root])
            .stream(store, &NoCache)
            .try_for_each(|item| async move {
                item.to_cid()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Pull of a new snapshot when the client already has the full history:
    /// the client's `skip_subgraph_roots` ride the wire and the server prunes
    /// its walk there — only the delta is transferred, in a single round.
    /// Crucially this needs NO `bloom_implies_complete_subgraphs` on the
    /// server (that flag caused the historic round-2 stall): the server runs
    /// a plain default config.
    #[test_log::test(async_std::test)]
    async fn test_pull_skip_roots_transfers_only_delta() -> TestResult {
        let server_store = &MemoryBlockStore::new();
        let client_store = &MemoryBlockStore::new();

        // Shared history: two snapshots, fully present on both sides
        let (snap1, _) = put_snapshot(server_store, None, 100, "s1").await?;
        let (snap1_client, _) = put_snapshot(client_store, None, 100, "s1").await?;
        let (snap2, _) = put_snapshot(server_store, Some(snap1), 100, "s2").await?;
        let (snap2_client, _) = put_snapshot(client_store, Some(snap1_client), 100, "s2").await?;
        assert_eq!(snap2, snap2_client);

        // New snapshot only on the server
        let (snap3, snap3_blocks) = put_snapshot(server_store, Some(snap2), 10, "s3").await?;

        let client_config = &Config {
            skip_subgraph_roots: HashSet::from([snap2]),
            ..Config::default()
        };
        let server_config = &Config::default();

        let mut rounds = 0;
        let mut blocks_transferred = 0;
        let mut request = pull::request(snap3, None, client_config, client_store, &NoCache).await?;
        assert_eq!(
            request.skip_subgraph_roots,
            vec![snap2],
            "round-1 request should carry the skip root on the wire"
        );
        while !request.indicates_finished() {
            rounds += 1;
            let response =
                pull::response(snap3, request, server_config, server_store, NoCache).await?;
            blocks_transferred += count_car_blocks(&response).await?;
            request =
                pull::request(snap3, Some(response), client_config, client_store, &NoCache).await?;
        }

        assert_eq!(rounds, 1, "delta pull should complete in a single round");
        assert_eq!(
            blocks_transferred, snap3_blocks,
            "only the new snapshot's blocks should be transferred"
        );
        assert_complete_dag(snap3, client_store).await?;

        Ok(())
    }

    /// Regression for the historic pull stall (proxy commit 707c13a): from
    /// round 2 the client HAS the root block, so the root is a bloom hit /
    /// skip candidate — validation must still reach the deeper requested
    /// roots. A small per-round budget forces many rounds; the transfer must
    /// converge and still never re-send pruned history.
    #[test_log::test(async_std::test)]
    async fn test_pull_skip_roots_multiround_converges_without_stall() -> TestResult {
        let server_store = &MemoryBlockStore::new();
        let client_store = &MemoryBlockStore::new();

        let (snap1, _) = put_snapshot(server_store, None, 200, "s1").await?;
        let (snap1_client, _) = put_snapshot(client_store, None, 200, "s1").await?;
        assert_eq!(snap1, snap1_client);

        // Large delta → several rounds at a small receive_maximum
        let (snap2, snap2_blocks) = put_snapshot(server_store, Some(snap1), 300, "s2").await?;

        let client_config = &Config {
            skip_subgraph_roots: HashSet::from([snap1]),
            receive_maximum: 16 * 1024,
            ..Config::default()
        };
        let server_config = &Config {
            receive_maximum: 16 * 1024,
            ..Config::default()
        };

        let mut rounds = 0;
        let mut blocks_transferred = 0;
        let mut request = pull::request(snap2, None, client_config, client_store, &NoCache).await?;
        while !request.indicates_finished() {
            rounds += 1;
            assert!(rounds < 200, "transfer failed to converge (stall)");
            let response =
                pull::response(snap2, request, server_config, server_store, NoCache).await?;
            request = pull::request(
                snap2,
                Some(response.clone()),
                client_config,
                client_store,
                &NoCache,
            )
            .await?;
            blocks_transferred += count_car_blocks(&response).await?;
        }

        assert!(rounds > 1, "test should exercise the multi-round path");
        assert_eq!(
            blocks_transferred, snap2_blocks,
            "history below the skip root must never be transferred"
        );
        assert_complete_dag(snap2, client_store).await?;

        Ok(())
    }

    /// Explicitly requested roots always win over a skip claim: a client that
    /// discovers a gap below a root it previously declared as skipped (wrong
    /// claim, lost data, …) can still request the missing blocks — this is
    /// the self-healing path.
    #[test_log::test(async_std::test)]
    async fn test_pull_explicit_request_bypasses_skip_roots() -> TestResult {
        let server_store = &MemoryBlockStore::new();

        let (snap1, _) = put_snapshot(server_store, None, 5, "s1").await?;
        let (snap2, _) = put_snapshot(server_store, Some(snap1), 5, "s2").await?;

        // A block deep below snap1 (which the request also declares skipped)
        let deep_cid = {
            let refs = NoCache.references(snap1, server_store).await?;
            refs[0] // the applogs chunk under snap1
        };

        let request = PullRequest {
            resources: vec![deep_cid],
            bloom_hash_count: 3,
            bloom_bytes: vec![],
            skip_subgraph_roots: vec![snap1],
        };

        let response =
            pull::response(snap2, request, &Config::default(), server_store, NoCache).await?;
        let reader = CarReader::new(Cursor::new(response.bytes.clone())).await?;
        let mut got = HashSet::new();
        let mut stream = Box::pin(reader.stream());
        while let Some((cid, _)) = stream.try_next().await? {
            got.insert(cid);
        }

        assert!(
            got.contains(&deep_cid),
            "explicitly requested root below a skip root must still be served"
        );

        Ok(())
    }

    /// Shallow pull: the client has NOTHING and declares the previous
    /// snapshot root as skipped because it doesn't WANT the history (bounded
    /// sync). Only the new snapshot's own blocks arrive, the protocol
    /// finishes cleanly, and the local DAG is intentionally incomplete below
    /// the skip root.
    #[test_log::test(async_std::test)]
    async fn test_pull_shallow_skips_unwanted_history() -> TestResult {
        let server_store = &MemoryBlockStore::new();
        let client_store = &MemoryBlockStore::new();

        let (snap1, _) = put_snapshot(server_store, None, 100, "s1").await?;
        let (snap2, snap2_blocks) = put_snapshot(server_store, Some(snap1), 10, "s2").await?;

        let client_config = &Config {
            skip_subgraph_roots: HashSet::from([snap1]),
            ..Config::default()
        };
        let server_config = &Config::default();

        let mut rounds = 0;
        let mut blocks_transferred = 0;
        let mut request = pull::request(snap2, None, client_config, client_store, &NoCache).await?;
        while !request.indicates_finished() {
            rounds += 1;
            assert!(rounds < 10, "shallow pull failed to converge");
            let response =
                pull::response(snap2, request, server_config, server_store, NoCache).await?;
            blocks_transferred += count_car_blocks(&response).await?;
            request =
                pull::request(snap2, Some(response), client_config, client_store, &NoCache).await?;
        }

        assert_eq!(
            blocks_transferred, snap2_blocks,
            "only the new snapshot's own blocks should be transferred"
        );
        // The new snapshot's own blocks are present…
        assert!(client_store.has_block(&snap2).await?);
        // …but the skipped history is genuinely absent (shallow by intent).
        assert!(!client_store.has_block(&snap1).await?);

        Ok(())
    }

    /// Push of a new snapshot when the server already has the full history
    /// (pinned roots as boundary): the server never asks for blocks below the
    /// boundary, and the transfer converges with the full DAG on the server.
    #[test_log::test(async_std::test)]
    async fn test_push_boundary_never_requests_history() -> TestResult {
        let server_store = &MemoryBlockStore::new();
        let client_store = &MemoryBlockStore::new();

        // Shared history
        let (snap1, _) = put_snapshot(server_store, None, 100, "s1").await?;
        let (snap1_client, _) = put_snapshot(client_store, None, 100, "s1").await?;
        assert_eq!(snap1, snap1_client);

        // History-only CIDs (for asserting the server never requests them)
        let history_cids: HashSet<Cid> = DagWalk::breadth_first([snap1])
            .stream(server_store, &NoCache)
            .and_then(|item| async move { item.to_cid() })
            .try_collect()
            .await?;

        // New snapshot only on the client
        let (snap2, _) = put_snapshot(client_store, Some(snap1_client), 10, "s2").await?;

        // Server treats its pinned root as complete boundary
        let server_config = &Config {
            skip_subgraph_roots: HashSet::from([snap1]),
            ..Config::default()
        };
        let client_config = &Config::default();

        let mut last_response = None;
        loop {
            let car =
                push::request(snap2, last_response, client_config, client_store, &NoCache).await?;
            let response = push::response(snap2, car, server_config, server_store, NoCache).await?;

            for requested in &response.subgraph_roots {
                assert!(
                    !history_cids.contains(requested),
                    "server requested history block {requested} despite boundary"
                );
            }

            if response.indicates_finished() {
                break;
            }
            last_response = Some(response);
        }

        assert_complete_dag(snap2, server_store).await?;

        Ok(())
    }

    /// A boundary must not break cold transfers (client has nothing at all):
    /// the server pruning flag is on, but the client sends no bloom, so
    /// everything is transferred as usual.
    #[test_log::test(async_std::test)]
    async fn test_pull_cold_transfer_with_pruning_enabled() -> TestResult {
        let server_store = &MemoryBlockStore::new();
        let client_store = &MemoryBlockStore::new();

        let (snap1, _) = put_snapshot(server_store, None, 50, "s1").await?;
        let (snap2, _) = put_snapshot(server_store, Some(snap1), 50, "s2").await?;

        let server_config = &Config {
            bloom_implies_complete_subgraphs: true,
            ..Config::default()
        };
        let client_config = &Config::default();

        let mut request = pull::request(snap2, None, client_config, client_store, &NoCache).await?;
        while !request.indicates_finished() {
            let response =
                pull::response(snap2, request, server_config, server_store, NoCache).await?;
            request =
                pull::request(snap2, Some(response), client_config, client_store, &NoCache).await?;
        }

        assert_complete_dag(snap2, client_store).await?;

        Ok(())
    }
}
