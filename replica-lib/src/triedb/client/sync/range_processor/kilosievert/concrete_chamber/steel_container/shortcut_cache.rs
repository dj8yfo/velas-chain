use std::collections::HashMap;

use evm_state::BlockNum;

use crate::triedb::client::sync::range_processor::kilosievert::concrete_chamber::StageOnePayload;

pub(super) enum ShortCut {
    ShortCutSaved(StageOnePayload),
    HeightReady,
    HeightSentForApply,
    CacheDropped,
    CacheDroppedHeightReady,
}

pub struct ShortcutRequestCache {
    data: HashMap<BlockNum, ShortCut>,
}

impl ShortcutRequestCache {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
    pub fn ready(&self, height: BlockNum) -> bool {
        match self.data.get(&height) {
            Some(ShortCut::HeightReady)
            | Some(ShortCut::HeightSentForApply)
            | Some(ShortCut::CacheDroppedHeightReady) => true,
            _ => false,
        }
    }
    pub fn sent_for_apply(&self, height: BlockNum) -> bool {
        match self.data.get(&height) {
            Some(ShortCut::HeightSentForApply)  => true,
            _ => false,
        }
    }

    pub fn height_ready(&mut self, height: BlockNum) -> Option<StageOnePayload> {
        let previous = self.data.insert(height, ShortCut::HeightReady);

        match previous {
            None => None,
            Some(ShortCut::ShortCutSaved(payload)) => {
                self.data.insert(height, ShortCut::HeightSentForApply);
                Some(payload)
            }
            Some(ShortCut::HeightReady) => None,
            Some(ShortCut::HeightSentForApply) => {
                self.data.insert(height, ShortCut::HeightSentForApply);
                None
            }
            Some(ShortCut::CacheDropped) | Some(ShortCut::CacheDroppedHeightReady) => {
                self.data.insert(height, ShortCut::CacheDroppedHeightReady);
                None
            }
        }
    }

    pub fn save_shortcut(
        &mut self,
        height: BlockNum,
        payload: StageOnePayload,
    ) -> Option<StageOnePayload> {
        let previous = self.data.insert(height, ShortCut::ShortCutSaved(payload));

        match previous {
            None => None,
            Some(ShortCut::ShortCutSaved(..)) => None,
            Some(ShortCut::CacheDropped) => {
                self.data.insert(height, ShortCut::CacheDropped);
                None
            }
            Some(ShortCut::CacheDroppedHeightReady) => {
                self.data.insert(height, ShortCut::CacheDroppedHeightReady);
                None
            }
            Some(ShortCut::HeightReady) => {
                let data = self.data.insert(height, ShortCut::HeightSentForApply);
                match data {
                    Some(ShortCut::ShortCutSaved(payload)) => Some(payload),
                    _ => unreachable!("not possible"),
                }
            }
            Some(ShortCut::HeightSentForApply) => {
                self.data.insert(height, ShortCut::HeightSentForApply);
                None
            }
        }
    }
    pub fn phase_out(&mut self, height: BlockNum) {
        let prev = self.data.insert(height, ShortCut::CacheDropped);
        match prev {
            Some(ShortCut::HeightReady) | Some(ShortCut::HeightSentForApply) => {
                self.data.insert(height, ShortCut::CacheDroppedHeightReady);
            },
            _ => {}
        }
    }
}
