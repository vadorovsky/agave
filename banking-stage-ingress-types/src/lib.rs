use {crossbeam_channel::Receiver, solana_perf::packet::PinnedPacketBatch, std::sync::Arc};

pub type BankingPacketBatch = Arc<Vec<PinnedPacketBatch>>;
pub type BankingPacketReceiver = Receiver<BankingPacketBatch>;
