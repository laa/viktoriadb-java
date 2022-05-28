package io.viktoriadb.exceptions;

public class BucketAlreadyExistException extends DbException {
    public BucketAlreadyExistException() {
        super("Bucket already exist");
    }
}
