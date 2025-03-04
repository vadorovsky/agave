use {
    crossbeam_channel::Receiver,
    solana_perf::packet::{PacketBatch, PacketRead, TpuPacketBatch},
    std::sync::Arc,
};

pub type BankingPacketBatch = Arc<Vec<PacketBatch>>;
pub type BankingPacketReceiver = Receiver<BankingPacketBatch>;
pub type TpuBankingPacketBatch = Arc<Vec<TpuPacketBatch>>;
pub type TpuBankingPacketReceiver = Receiver<TpuBankingPacketBatch>;
