package io.viktoriadb.exceptions;

public class KeyRequiredException extends DbException {
    public KeyRequiredException() {
        super("key is required");
    }

}
