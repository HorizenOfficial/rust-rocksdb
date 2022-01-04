use crate::transactions::transaction::Transaction;
use crate::Error;

pub trait TransactionBegin: Sized {
    type WriteOptions: Default;
    type TransactionOptions: Default;
    fn transaction(
        &self,
        write_options: &<Self as TransactionBegin>::WriteOptions,
        tx_options: &<Self as TransactionBegin>::TransactionOptions,
    ) -> Result<Transaction, Error>;

    /// Begins a new optimistic transaction with default options.
    fn transaction_default(&self) -> Result<Transaction, Error> {
        let write_options = Self::WriteOptions::default();
        let transaction_options = Self::TransactionOptions::default();
        self.transaction(&write_options, &transaction_options)
    }
}
