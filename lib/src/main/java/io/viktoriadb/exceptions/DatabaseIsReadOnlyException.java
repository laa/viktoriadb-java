package io.viktoriadb.exceptions;

public class DatabaseIsReadOnlyException extends DbException {
    public DatabaseIsReadOnlyException() {
        super("Database is opened in read-only mode");
    }
}
