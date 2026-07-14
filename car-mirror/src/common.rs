use crate::{
    buffered::BufferedBlockStore,
    cache::Cache,
    dag_walk::DagWalk,
    error::Error,
    incremental_verification::{BlockState, IncrementalDagVerification},
    messages::{PullRequest, PushResponse},
};
use bytes::Bytes;
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
    /// Roots of subgraphs that are known to be *completely* present on the
    /// block-receiving side (application invariant — e.g. wovin snapshot roots,
    /// which are only ever recorded/pinned as complete DAGs).
    ///
    /// Used during incremental verification: traversal stops at these CIDs
    /// instead of walking the entire (append-only) history below them, making
    /// verification cost proportional to the new data instead of the full DAG.
    ///
    /// This is per-transfer state more than static configuration — clone the
    /// base config and fill this in per request. Empty by default (no effect).
    pub complete_subgraph_roots: HashSet<Cid>,
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
            complete_subgraph_roots: HashSet::new(),
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
    } = last_state.unwrap_or(ReceiverState {
        missing_subgraph_roots: vec![root],
        have_cids_bloom: None,
    });

    let bloom = handle_missing_bloom(have_cids_bloom);

    // Verify that all missing subgraph roots are in the relevant DAG.
    // Short-circuit: if the only root requested is the DAG root itself,
    // it's trivially valid — skip the expensive full-DAG walk.
    let subgraph_roots = if missing_subgraph_roots == [root] {
        missing_subgraph_roots
    } else {
        verify_missing_subgraph_roots(
            root,
            &missing_subgraph_roots,
            &bloom,
            prune_bloom_subgraphs,
            &store,
            &cache,
        )
        .await?
    };

    let stream =
        stream_blocks_from_roots(subgraph_roots, bloom, prune_bloom_subgraphs, store, cache);

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
            config.complete_subgraph_roots.clone(),
            &store,
            &cache,
        )
        .await?
        .into_receiver_state(config.bloom_fpr),
    };

    receiver_state
        .missing_subgraph_roots
        .truncate(config.max_roots_per_round);

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
        config.complete_subgraph_roots.clone(),
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
                // This can happen because we've just discovered a subgraph we already have.
                // Let's update the endpoint with our new receiver state.
                tracing::debug!(%cid, "Received block we already have, stopping transfer");
                break;
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

    Ok(dag_verification.into_receiver_state(config.bloom_fpr))
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
            config.complete_subgraph_roots.clone(),
            &buffered,
            &cache,
        )
        .await?
        .into_receiver_state(config.bloom_fpr),
    };

    receiver_state
        .missing_subgraph_roots
        .truncate(config.max_roots_per_round);

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
async fn verify_missing_subgraph_roots(
    root: Cid,
    missing_subgraph_roots: &[Cid],
    bloom: &BloomFilter,
    prune_bloom_subgraphs: bool,
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
            continue;
        }

        // If the receiver has this block (bloom hit) and its subgraph is
        // implied complete, don't walk below it — but never prune away CIDs
        // the receiver explicitly asked for (they're always valid to request).
        if prune_bloom_subgraphs && bloom.contains(&cid.to_bytes()) {
            prune_frontier_below(&mut dag_walk, cid, &missing_set, store, cache).await?;
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

fn stream_blocks_from_roots<'a>(
    subgraph_roots: Vec<Cid>,
    bloom: BloomFilter,
    prune_bloom_subgraphs: bool,
    store: impl BlockStore + 'a,
    cache: impl Cache + 'a,
) -> BlockStream<'a> {
    let subgraph_roots_set: HashSet<Cid> = subgraph_roots.iter().cloned().collect();
    Box::pin(async_stream::try_stream! {
        let mut dag_walk = DagWalk::breadth_first(subgraph_roots.clone());

        while let Some(item) = dag_walk.next(&store, &cache).await? {
            let cid = item.to_cid()?;

            if should_block_be_skipped(&cid, &bloom, &subgraph_roots_set) {
                if prune_bloom_subgraphs {
                    // Receiver has this block, and with the complete-subgraph
                    // invariant it has everything below it too — stop walking
                    // (but never prune explicitly requested roots).
                    prune_frontier_below(&mut dag_walk, cid, &subgraph_roots_set, &store, &cache).await?;
                }
                continue;
            }

            let bytes = store.get_block(&cid).await.map_err(Error::BlockStoreError)?;

            yield (cid, bytes);
        }
    })
}

/// Remove `cid`'s direct references from the walk frontier (they were just
/// enqueued by `dag_walk.next`), keeping any CIDs in `protected`.
///
/// Used when the receiver is known to have `cid`'s complete subgraph
/// (see [`Config::bloom_implies_complete_subgraphs`]).
async fn prune_frontier_below(
    dag_walk: &mut DagWalk,
    cid: Cid,
    protected: &HashSet<Cid>,
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<(), Error> {
    let refs: HashSet<Cid> = cache
        .references(cid, store)
        .await
        .map_err(Error::BlockStoreError)?
        .into_iter()
        .collect();
    dag_walk
        .frontier
        .retain(|c| protected.contains(c) || !refs.contains(c));
    Ok(())
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
        } = push;

        Self {
            missing_subgraph_roots: subgraph_roots,
            have_cids_bloom: Self::bloom_deserialize(hash_count, bytes),
        }
    }
}

impl From<PullRequest> for ReceiverState {
    fn from(pull: PullRequest) -> Self {
        let PullRequest {
            resources,
            bloom_hash_count: hash_count,
            bloom_bytes: bytes,
        } = pull;

        Self {
            missing_subgraph_roots: resources,
            have_cids_bloom: Self::bloom_deserialize(hash_count, bytes),
        }
    }
}

impl From<ReceiverState> for PushResponse {
    fn from(receiver_state: ReceiverState) -> PushResponse {
        let ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom,
        } = receiver_state;

        let (hash_count, bytes) = ReceiverState::bloom_serialize(have_cids_bloom);

        PushResponse {
            subgraph_roots: missing_subgraph_roots,
            bloom_hash_count: hash_count,
            bloom_bytes: bytes,
        }
    }
}

impl From<ReceiverState> for PullRequest {
    fn from(receiver_state: ReceiverState) -> PullRequest {
        let ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom,
        } = receiver_state;

        let (hash_count, bytes) = ReceiverState::bloom_serialize(have_cids_bloom);

        PullRequest {
            resources: missing_subgraph_roots,
            bloom_hash_count: hash_count,
            bloom_bytes: bytes,
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
    /// with the client seeding its boundary and the server pruning below
    /// bloom hits, only the delta is transferred — in a single round.
    #[test_log::test(async_std::test)]
    async fn test_pull_boundary_transfers_only_delta() -> TestResult {
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
            complete_subgraph_roots: HashSet::from([snap2]),
            ..Config::default()
        };
        let server_config = &Config {
            bloom_implies_complete_subgraphs: true,
            ..Config::default()
        };

        let mut rounds = 0;
        let mut blocks_transferred = 0;
        let mut request = pull::request(snap3, None, client_config, client_store, &NoCache).await?;
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
            complete_subgraph_roots: HashSet::from([snap1]),
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
