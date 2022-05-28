package io.viktoriadb.exceptions;

import io.viktoriadb.DB;

public class InvalidMagicException extends DbException {
    public InvalidMagicException() {
        super("Invalid magic number");
    }
}
