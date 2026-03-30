use bytes::Bytes;
use libipld_core::cid::Cid;
use std::sync::Mutex;
use wnfs_common::{utils::CondSend, BlockStore, BlockStoreError};

/// A block store wrapper that buffers `put_block_keyed` calls in memory
/// instead of forwarding them to the inner store immediately.
///
/// This is useful for block stores backed by remote APIs (e.g., IPFS/kubo via HTTP RPC)
/// where individual `put_block_keyed` calls would each result in a separate HTTP roundtrip.
///
/// Wrap your store in `BufferedBlockStore` before passing it to [`push::response`][crate::push::response]
/// or [`pull::request`][crate::pull::request]. After the protocol round completes, call
/// [`into_inner_and_blocks`][BufferedBlockStore::into_inner_and_blocks] to retrieve all
/// received blocks as a batch for efficient bulk storage (e.g., re-assembling a CAR file
/// for `dag/import`).
///
/// Reads (`get_block`, `has_block`) transparently check the buffer first, so
/// incremental DAG verification works correctly through the wrapper.
///
/// # Example
///
/// ```
/// use car_mirror::{push, common::Config, buffered::BufferedBlockStore};
/// use car_mirror::cache::NoCache;
/// use wnfs_common::MemoryBlockStore;
///
/// # async fn example() -> anyhow::Result<()> {
/// let inner_store = MemoryBlockStore::new();
/// let buffered = BufferedBlockStore::new(inner_store);
///
/// // Use buffered store with the push protocol
/// // let response = push::response(root, car_file, &config, &buffered, &NoCache).await?;
///
/// // Retrieve all blocks that were stored during the round
/// let (inner_store, blocks) = buffered.into_inner_and_blocks();
/// // Now batch-store `blocks: Vec<(Cid, Bytes)>` however you like
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct BufferedBlockStore<B> {
    inner: B,
    buffer: Mutex<Vec<(Cid, Bytes)>>,
}

impl<B> BufferedBlockStore<B> {
    /// Create a new buffered block store wrapping the given inner store.
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            buffer: Mutex::new(Vec::new()),
        }
    }

    /// Consume this wrapper, returning the inner store and all buffered blocks.
    ///
    /// The returned blocks are in the order they were stored during the protocol round.
    pub fn into_inner_and_blocks(self) -> (B, Vec<(Cid, Bytes)>) {
        let blocks = self.buffer.into_inner().expect("mutex not poisoned");
        (self.inner, blocks)
    }

    /// Get a snapshot of the currently buffered blocks.
    pub fn buffered_blocks(&self) -> Vec<(Cid, Bytes)> {
        self.buffer.lock().expect("mutex not poisoned").clone()
    }
}

impl<B: BlockStore> BlockStore for BufferedBlockStore<B> {
    async fn get_block(&self, cid: &Cid) -> Result<Bytes, BlockStoreError> {
        // Check buffer first
        {
            let buffer = self.buffer.lock().expect("mutex not poisoned");
            if let Some((_, bytes)) = buffer.iter().find(|(c, _)| c == cid) {
                return Ok(bytes.clone());
            }
        }
        self.inner.get_block(cid).await
    }

    async fn put_block_keyed(
        &self,
        cid: Cid,
        bytes: impl Into<Bytes> + CondSend,
    ) -> Result<(), BlockStoreError> {
        let bytes = bytes.into();
        self.buffer
            .lock()
            .expect("mutex not poisoned")
            .push((cid, bytes));
        Ok(())
    }

    async fn has_block(&self, cid: &Cid) -> Result<bool, BlockStoreError> {
        {
            let buffer = self.buffer.lock().expect("mutex not poisoned");
            if buffer.iter().any(|(c, _)| c == cid) {
                return Ok(true);
            }
        }
        self.inner.has_block(cid).await
    }

    async fn put_block(
        &self,
        bytes: impl Into<Bytes> + CondSend,
        codec: u64,
    ) -> Result<Cid, BlockStoreError> {
        let bytes: Bytes = bytes.into();
        let cid = self.inner.create_cid(&bytes, codec)?;
        self.buffer
            .lock()
            .expect("mutex not poisoned")
            .push((cid, bytes));
        Ok(cid)
    }

    fn create_cid(&self, bytes: &[u8], codec: u64) -> Result<Cid, BlockStoreError> {
        self.inner.create_cid(bytes, codec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cache::NoCache,
        common::Config,
        dag_walk::DagWalk,
        push,
        test_utils::setup_random_dag,
    };
    use futures::TryStreamExt;
    use std::collections::HashSet;
    use testresult::TestResult;
    use wnfs_common::MemoryBlockStore;

    #[test_log::test(async_std::test)]
    async fn test_buffered_push_transfer() -> TestResult {
        let (root, ref client_store) = setup_random_dag(256, 10 * 1024).await?;
        let config = &Config::default();

        // Use a BufferedBlockStore so blocks accumulate in memory
        // instead of going to the inner store immediately.
        let server_store = MemoryBlockStore::new();
        let buffered = BufferedBlockStore::new(&server_store);

        // Run the first push round through the buffer
        let car_file = push::request(root, None, config, client_store, &NoCache).await?;
        let response = push::response(root, car_file, config, &buffered, &NoCache).await?;

        // Blocks should be in the buffer, NOT in the inner store
        let blocks = buffered.buffered_blocks();
        assert!(!blocks.is_empty(), "buffer should contain blocks");

        // The inner store should still be empty (blocks were buffered)
        assert!(
            !server_store.has_block(&root).await.unwrap_or(false),
            "inner store should not have blocks yet"
        );

        // Now flush: store all buffered blocks in the real store
        let (_, blocks) = buffered.into_inner_and_blocks();
        for (cid, bytes) in &blocks {
            server_store.put_block_keyed(*cid, bytes.clone()).await?;
        }

        // Continue the protocol to completion using the real store
        if !response.indicates_finished() {
            let mut request =
                push::request(root, Some(response), config, client_store, &NoCache).await?;
            loop {
                let response =
                    push::response(root, request, config, &server_store, &NoCache).await?;
                if response.indicates_finished() {
                    break;
                }
                request =
                    push::request(root, Some(response), config, client_store, &NoCache).await?;
            }
        }

        // Verify all blocks transferred
        let client_cids = DagWalk::breadth_first([root])
            .stream(client_store, &NoCache)
            .and_then(|item| async move { item.to_cid() })
            .try_collect::<HashSet<_>>()
            .await?;
        let server_cids = DagWalk::breadth_first([root])
            .stream(&server_store, &NoCache)
            .and_then(|item| async move { item.to_cid() })
            .try_collect::<HashSet<_>>()
            .await?;

        assert_eq!(client_cids, server_cids);

        Ok(())
    }
}
