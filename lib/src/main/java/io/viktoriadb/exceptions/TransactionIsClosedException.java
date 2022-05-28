package io.viktoriadb.exceptions;

public class TransactionIsClosedException extends DbException {
    public TransactionIsClosedException() {
        super("Transaction is closed");
    }
}
