package io.viktoriadb.exceptions;

public class TransactionIsNotWritableException extends DbException {
    public TransactionIsNotWritableException() {
        super("Transaction is not writable");
    }
}
