package io.viktoriadb.operations.log;

@FunctionalInterface
public interface SegmentCloseListener {
    void onClose();
}
