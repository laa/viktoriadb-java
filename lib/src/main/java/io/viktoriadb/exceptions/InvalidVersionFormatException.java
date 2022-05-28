package io.viktoriadb.exceptions;

public class InvalidVersionFormatException extends DbException {
    public InvalidVersionFormatException() {
        super("Invalid version format");
    }
}
