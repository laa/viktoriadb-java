package io.viktoriadb.exceptions;

public class KeyTooLargeException extends DbException {
    public KeyTooLargeException() {
        super("Key too large");
    }
}
