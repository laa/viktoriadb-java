package io.viktoriadb.exceptions;

public class BucketNotFoundException extends DbException {
    public BucketNotFoundException() {
        super("Bucket not found");
    }
}
