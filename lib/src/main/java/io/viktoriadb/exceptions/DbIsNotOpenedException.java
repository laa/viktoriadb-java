package io.viktoriadb.exceptions;

public class DbIsNotOpenedException extends DbException {
    public DbIsNotOpenedException() {
        super("Database is not opened");
    }
}
