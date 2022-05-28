package io.viktoriadb.exceptions;

public class InvalidCheckSumException extends DbException{
    public InvalidCheckSumException() {
        super("Invalid check sum");
    }
}
