package io.viktoriadb.operations.log;

import io.viktoriadb.operations.Operation;

public final class OperationsLog {
    private final int segmentSize;

    public OperationsLog(int segmentSize) {
        this.segmentSize = segmentSize;
    }

    public long log(final Operation operation) {
        throw new UnsupportedOperationException();
    }

    public <T extends Operation> T load(long id) {
        throw new UnsupportedOperationException();
    }

    public void truncateTill(long id) {
        throw new UnsupportedOperationException();
    }

    public void flush() {
        throw new UnsupportedOperationException();
    }
}
