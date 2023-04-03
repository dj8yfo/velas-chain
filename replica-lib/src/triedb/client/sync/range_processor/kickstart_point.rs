use std::sync::{Arc, Mutex};

use evm_state::{BlockNum, H256};

#[derive(Clone)]
pub struct KickStartPoint {
    data: Arc<Mutex<Entry>>,
}

#[derive(Clone, Copy)]
pub struct Entry {
    pub height: BlockNum,
    pub hash: H256,
    pub upward: bool,
}

impl KickStartPoint {
    pub fn new(height: BlockNum, hash: H256, upward: bool) -> Self {
        let entry = Entry {
            height,
            hash,
            upward,
        };

        Self {
            data: Arc::new(Mutex::new(entry)),
        }
    }

    pub fn get(&self) -> Entry {
        *self.data.lock().expect("poison")
    }

    pub fn update(&self, height: BlockNum, hash: H256) {
        let mut lock = self.data.lock().expect("poison");
        if lock.upward {
            if height > lock.height {
                lock.height = height;
                lock.hash = hash;
            }
        } else {
            if height < lock.height {
                lock.height = height;
                lock.hash = hash;
            }
        }
    }
}

#[derive(Clone)]
pub struct SuccessHeights {
    data: Arc<Mutex<Vec<BlockNum>>>,
}

impl SuccessHeights {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(vec![])),
        }
        
    }
    pub fn push_height(&self, h: BlockNum) {
        self.data.lock().expect("poison").push(h);
        
    }
    pub fn take(&self) -> Vec<BlockNum> {
        self.data.lock().expect("poison").drain(..).collect()
        
    }
    
}
