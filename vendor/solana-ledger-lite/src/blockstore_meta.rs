#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ErasureConfig {
    pub(crate) num_data: usize,
    pub(crate) num_coding: usize,
}

impl ErasureConfig {
    pub(crate) fn is_fixed(&self) -> bool {
        self.num_data == crate::shred::DATA_SHREDS_PER_FEC_BLOCK
            && self.num_coding == crate::shred::DATA_SHREDS_PER_FEC_BLOCK
    }
}
