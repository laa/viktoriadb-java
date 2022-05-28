package io.viktoriadb.exceptions;

public class IncompatibleValueException extends DbException {
    public IncompatibleValueException() {
        super("Incompatible value");
    }
}
