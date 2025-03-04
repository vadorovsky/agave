use {
    crossbeam_channel::Receiver,
    solana_perf::packet::{PacketBatch, TpuPacket},
    std::sync::Arc,
};

pub type BankingPacketBatch = Arc<Vec<PacketBatch>>;
pub type BankingPacketReceiver = Receiver<BankingPacketBatch>;
pub type TpuBankingPacketBatch = Arc<Vec<Vec<TpuPacket>>>;
pub type TpuBankingPacketReceiver = Receiver<TpuBankingPacketBatch>;
