package io.viktoriadb.exceptions;

public class ValueTooLargeException extends DbException {
    public ValueTooLargeException() {
        super("Value to large exception");
    }
}
