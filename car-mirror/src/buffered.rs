use bytes::Bytes;
use libipld_core::cid::Cid;
use std::sync::Mutex;
use wnfs_common::{utils::CondSend, BlockStore, BlockStoreError};

/// An internal block store overlay that captures writes in memory
/// while delegating reads to both the buffer and the inner store.
///
/// Used as an implementation detail of the `_with_blocks` protocol functions.
#[derive(Debug)]
pub(crate) struct BufferedBlockStore<B> {
    inner: B,
    buffer: Mutex<Vec<(Cid, Bytes)>>,
}

impl<B> BufferedBlockStore<B> {
    pub(crate) fn new(inner: B) -> Self {
        Self {
            inner,
            buffer: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn into_blocks(self) -> Vec<(Cid, Bytes)> {
        self.buffer.into_inner().expect("mutex not poisoned")
    }
}

impl<B: BlockStore> BlockStore for BufferedBlockStore<B> {
    async fn get_block(&self, cid: &Cid) -> Result<Bytes, BlockStoreError> {
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
