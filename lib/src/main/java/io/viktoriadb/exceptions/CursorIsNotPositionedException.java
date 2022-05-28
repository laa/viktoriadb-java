package io.viktoriadb.exceptions;

public class CursorIsNotPositionedException extends DbException {
    public CursorIsNotPositionedException() {
        super("Cursor is not positioned");
    }
}
