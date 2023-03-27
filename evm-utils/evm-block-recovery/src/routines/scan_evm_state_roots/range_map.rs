use std::{path::{Path, PathBuf}, sync::{Mutex, Arc, MutexGuard}};

use evm_state::BlockNum;
use rangemap::RangeMap;

#[derive(Debug, Clone)]
pub struct MasterRange {
    file_path: PathBuf,
    inner: Arc<Mutex<(u64, RangeMap<BlockNum, String>)>>,

}

impl MasterRange {
    pub fn new(file_path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let ser = std::fs::read_to_string(file_path.as_ref())?;
        let i: RangeMap<BlockNum, String> = serde_json::from_str(&ser)?;
        log::info!("MasterRange::new {:#?}", i);
        Ok(Self {
            inner: Arc::new(Mutex::new((0, i))),
            file_path: file_path.as_ref().to_owned(),
        })
    }

    pub fn update(&self, index: BlockNum, value: String) -> std::io::Result<()> {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.1.insert(index..index + 1, value);
        inner.0 += 1;
        if inner.0 % 1000 == 0 {
            Self::persist(self.file_path.clone(), inner)?;
        }
        Ok(())
    }

    fn persist(file_path: PathBuf, inner: MutexGuard<(u64, RangeMap<BlockNum, String>)>) -> std::io::Result<()> {
        let content = serde_json::to_string_pretty(&(*inner).1).unwrap();
        std::fs::write(file_path, content.as_bytes())?;
        Ok(())
    }

    pub fn persist_external(&self) -> std::io::Result<()> {
        let inner = self.inner.lock().expect("poisoned");
        let content = serde_json::to_string_pretty(&(*inner).1).unwrap();
        std::fs::write(&self.file_path, content.as_bytes())?;
        Ok(())
    }
}
