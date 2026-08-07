use crate::{
    cache::Cache,
    common::{self, ReceiverState},
    error::{Error, IncrementalVerificationError},
};
use bytes::Bytes;
use deterministic_bloom::runtime_size::BloomFilter;
use libipld_core::{
    cid::Cid,
    multihash::{Code, MultihashDigest},
};
use std::{
    collections::{HashSet, VecDeque},
    matches,
};
use wnfs_common::BlockStore;

/// A data structure that keeps state about incremental DAG verification.
#[derive(Clone, Debug)]
pub struct IncrementalDagVerification {
    /// All the CIDs that have been discovered to be missing from the DAG.
    pub want_cids: HashSet<Cid>,
    /// All the CIDs that are available locally.
    ///
    /// This only ever contains CIDs actually walked/received during this
    /// verification — boundary members (see [`Self::boundary`]) are tracked
    /// separately and never end up in here, so blooms built from this set
    /// stay proportional to the transfer delta, not the boundary size.
    pub have_cids: HashSet<Cid>,
    /// The complete-subgraph boundary: CIDs whose subgraphs the caller asserts
    /// to be completely present locally. Traversal never descends below a
    /// member; membership is only *checked* (this set may be huge — e.g. every
    /// pinned snapshot root on a server) and is never copied into
    /// [`Self::have_cids`] nor into the receiver-state bloom.
    pub boundary: HashSet<Cid>,
    /// The subset of [`Self::boundary`] that was actually encountered during
    /// traversal. These are the only boundary members relevant to the current
    /// transfer, and the only ones that go on the wire (as
    /// `skip_subgraph_roots` in the receiver state).
    pub boundary_hits: HashSet<Cid>,
}

/// The state of a block retrieval
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockState {
    /// The block was already received/is already stored
    Have,
    /// We know we will need this block
    Want,
    /// We don't know whether we'll need this block
    Unexpected,
}

impl IncrementalDagVerification {
    /// Initiate incremental DAG verification of given roots.
    ///
    /// This will already run a traversal to find missing subgraphs and
    /// CIDs that are already present.
    pub async fn new(
        roots: impl IntoIterator<Item = Cid>,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<Self, Error> {
        Self::new_with_boundary(roots, HashSet::new(), store, cache).await
    }

    /// Like [`Self::new`], but with a set of "complete boundary" roots:
    /// CIDs whose subgraphs the caller asserts to be completely present
    /// locally (application invariant, e.g. wovin snapshot roots which are
    /// only ever recorded/pinned as complete DAGs).
    ///
    /// The boundary is used purely as a membership check during traversal:
    /// traversal stops at a member instead of walking the entire history
    /// below it (making verification cost proportional to the *new* data,
    /// not the full DAG), and the member is recorded in
    /// [`Self::boundary_hits`]. The boundary is never seeded into
    /// [`Self::have_cids`], so it never inflates the receiver-state bloom
    /// nor the wire — only the (few) actually-hit members do.
    ///
    /// A root that is itself a boundary member is trivially complete: it's
    /// recorded as a boundary hit, not a want.
    pub async fn new_with_boundary(
        roots: impl IntoIterator<Item = Cid>,
        complete_boundary: HashSet<Cid>,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<Self, Error> {
        let boundary = complete_boundary;
        let mut boundary_hits = HashSet::new();
        let want_cids = roots
            .into_iter()
            .filter(|root| {
                if boundary.contains(root) {
                    boundary_hits.insert(*root);
                    false
                } else {
                    true
                }
            })
            .collect();

        let mut this = Self {
            want_cids,
            have_cids: HashSet::new(),
            boundary,
            boundary_hits,
        };

        this.update_have_cids(store, cache).await?;

        Ok(this)
    }

    /// Updates the state of incremental dag verification.
    /// This goes through all "want" blocks and what they link to,
    /// removing items that we now have and don't want anymore.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn update_have_cids(
        &mut self,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<(), Error> {
        // Re-examine all current wants (they may have appeared in the store).
        let seeds: Vec<Cid> = self.want_cids.drain().collect();
        self.discover(seeds, store, cache).await?;

        tracing::debug!(
            num_want = self.want_cids.len(),
            num_have = self.have_cids.len(),
            "Finished dag verification"
        );

        Ok(())
    }

    /// BFS from the given seeds, classifying each newly-encountered CID as
    /// "have" (present locally, links followed) or "want" (missing, frontier).
    /// A boundary member is recorded as a boundary hit and never descended
    /// into (nor added to have/want). CIDs already classified are skipped —
    /// so across a whole transfer each CID is visited at most once (amortized
    /// linear), instead of re-walking the DAG per block.
    async fn discover(
        &mut self,
        seeds: impl IntoIterator<Item = Cid>,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<(), Error> {
        let mut queue: VecDeque<Cid> = seeds.into_iter().collect();

        while let Some(cid) = queue.pop_front() {
            if self.boundary.contains(&cid) {
                self.boundary_hits.insert(cid);
                continue;
            }

            if self.have_cids.contains(&cid) || self.want_cids.contains(&cid) {
                continue;
            }

            if store
                .has_block(&cid)
                .await
                .map_err(Error::BlockStoreError)?
            {
                self.mark_as_have(cid);
                let refs = cache
                    .references(cid, store)
                    .await
                    .map_err(Error::BlockStoreError)?;
                queue.extend(refs);
            } else {
                tracing::trace!(%cid, "Missing block, adding to want list");
                self.mark_as_want(cid);
            }
        }

        Ok(())
    }

    fn mark_as_want(&mut self, want: Cid) {
        if self.have_cids.contains(&want) {
            tracing::warn!(%want, "Marking a CID as wanted, that we have previously marked as having!");
            self.have_cids.remove(&want);
        }
        self.want_cids.insert(want);
    }

    fn mark_as_have(&mut self, have: Cid) {
        self.want_cids.remove(&have);
        self.have_cids.insert(have);
    }

    /// Check the state of a CID to find out whether
    /// - we expect it as one of the next possible blocks to receive (Want)
    /// - we have already stored it (Have)
    /// - we don't know whether we need it (Unexpected)
    pub fn block_state(&self, cid: Cid) -> BlockState {
        if self.want_cids.contains(&cid) {
            BlockState::Want
        } else if self.have_cids.contains(&cid) || self.boundary.contains(&cid) {
            // Boundary members count as "have": the receiver asserts it holds
            // their complete subgraphs, so it doesn't want them re-sent.
            BlockState::Have
        } else {
            BlockState::Unexpected
        }
    }

    /// Verify that
    /// - the block is part of the graph below the roots.
    /// - the block hasn't been received before
    /// - the block actually hashes to the hash from given CID and
    ///
    /// And finally stores the block in the blockstore.
    ///
    /// This *may* fail, even if the block is part of the graph below the roots,
    /// if intermediate blocks between the roots and this block are missing.
    ///
    /// This *may* add the block to the blockstore, but still fail to verify, specifically
    /// if the block's bytes don't match the hash in the CID.
    pub async fn verify_and_store_block(
        &mut self,
        block: (Cid, Bytes),
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<(), Error> {
        let (cid, bytes) = block;

        let block_state = self.block_state(cid);
        if !matches!(block_state, BlockState::Want) {
            return Err(IncrementalVerificationError::ExpectedWantedBlock {
                cid: Box::new(cid),
                block_state,
            }
            .into());
        }

        let hash_func: Code = cid
            .hash()
            .code()
            .try_into()
            .map_err(|_| Error::UnsupportedHashCode { cid })?;

        let hash = hash_func.digest(bytes.as_ref());

        if &hash != cid.hash() {
            let actual_cid = Cid::new_v1(cid.codec(), hash);
            return Err(IncrementalVerificationError::DigestMismatch {
                cid: Box::new(cid),
                actual_cid: Box::new(actual_cid),
            }
            .into());
        }

        store
            .put_block_keyed(cid, bytes.clone())
            .await
            .map_err(Error::BlockStoreError)?;

        // Incrementally update state: mark this CID as "have" and classify its
        // direct links via `discover` (which stops at boundary roots and skips
        // already-classified CIDs). Amortized O(links) per block instead of a
        // full DAG walk per block.
        self.mark_as_have(cid);

        let refs = common::references(cid, &bytes, Vec::new()).map_err(Error::ParsingError)?;
        self.discover(refs, store, cache).await?;

        Ok(())
    }

    /// Computes the receiver state for the current incremental dag verification state.
    ///
    /// The `have_cids_bloom` is built over the walked/received CIDs only
    /// (`have_cids`), so its size is proportional to the transfer delta.
    /// The (sorted) boundary hits are returned as `skip_subgraph_roots`, so
    /// the sender prunes its walk below exactly the boundary members that are
    /// relevant to this transfer — never the whole (potentially huge) boundary.
    pub fn into_receiver_state(self, bloom_fpr: fn(u64) -> f64) -> ReceiverState {
        let missing_subgraph_roots: Vec<Cid> = self.want_cids.into_iter().collect();

        let mut skip_subgraph_roots: Vec<Cid> = self.boundary_hits.into_iter().collect();
        skip_subgraph_roots.sort();

        let bloom_capacity = self.have_cids.len() as u64;

        if bloom_capacity == 0 {
            return ReceiverState {
                missing_subgraph_roots,
                have_cids_bloom: None,
                skip_subgraph_roots,
            };
        }

        if missing_subgraph_roots.is_empty() {
            // We're done. No need to compute a bloom.
            return ReceiverState {
                missing_subgraph_roots,
                have_cids_bloom: None,
                skip_subgraph_roots,
            };
        }

        let target_fpr = bloom_fpr(bloom_capacity);
        let mut bloom = BloomFilter::new_from_fpr_po2(bloom_capacity, target_fpr);

        self.have_cids
            .into_iter()
            .for_each(|cid| bloom.insert(&cid.to_bytes()));

        tracing::debug!(
            inserted_elements = bloom_capacity,
            size_bits = bloom.as_bytes().len() * 8,
            hash_count = bloom.hash_count(),
            ones_count = bloom.count_ones(),
            target_fpr,
            estimated_fpr = bloom.current_false_positive_rate(),
            "built 'have cids' bloom",
        );

        ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom: Some(bloom),
            skip_subgraph_roots,
        }
    }
}
