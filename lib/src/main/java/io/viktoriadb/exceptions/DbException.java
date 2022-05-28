package io.viktoriadb.exceptions;

public class DbException extends RuntimeException{
    public DbException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public DbException(String message) {
        super(message);
    }
}
