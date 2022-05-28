package io.viktoriadb.exceptions;

public class BucketNameRequiredException extends DbException {
    public BucketNameRequiredException() {
        super("Bucket name required");
    }
}
