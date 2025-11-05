use {
    serde::{Deserialize, Serialize},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_svm::transaction_processor::TransactionLogMessages,
    solana_transaction_error::TransactionResult as Result,
    std::collections::{HashMap, HashSet},
};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub enum TransactionLogCollectorFilter {
    All,
    AllWithVotes,
    #[default]
    None,
    OnlyMentionedAddresses,
}

#[derive(Debug, Default)]
pub struct TransactionLogCollectorConfig {
    pub mentioned_addresses: HashSet<Pubkey>,
    pub filter: TransactionLogCollectorFilter,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionLogInfo {
    pub signature: Signature,
    pub result: Result<()>,
    pub is_vote: bool,
    pub log_messages: TransactionLogMessages,
}

#[derive(Default, Debug)]
pub struct TransactionLogCollector {
    // All the logs collected for from this Bank.  Exact contents depend on the
    // active `TransactionLogCollectorFilter`
    pub logs: Vec<TransactionLogInfo>,

    // For each `mentioned_addresses`, maintain a list of indices into `logs` to easily
    // locate the logs from transactions that included the mentioned addresses.
    pub mentioned_address_map: HashMap<Pubkey, Vec<usize>>,
}

impl TransactionLogCollector {
    pub fn get_logs_for_address(
        &self,
        address: Option<&Pubkey>,
    ) -> Option<Vec<TransactionLogInfo>> {
        match address {
            None => Some(self.logs.clone()),
            Some(address) => self.mentioned_address_map.get(address).map(|log_indices| {
                log_indices
                    .iter()
                    .filter_map(|i| self.logs.get(*i).cloned())
                    .collect()
            }),
        }
    }
}
