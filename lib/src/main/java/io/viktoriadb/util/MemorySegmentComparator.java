package io.viktoriadb.util;

import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemorySegment;

import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Comparator;

/**
 * Comparator to compare {@link java.nio.ByteBuffer}s as if they contain unsigned byte arrays.
 */
public final class MemorySegmentComparator implements Comparator<MemorySegment> {
    private static final VarHandle BYTE_HANDLE = MemoryHandles.varHandle(byte.class, ByteOrder.nativeOrder());

    public static final MemorySegmentComparator INSTANCE = new MemorySegmentComparator();

    @Override
    public int compare(final MemorySegment firstSegment, final MemorySegment secondSegment) {
        long firstSize = firstSegment.byteSize();
        long secondSize = secondSegment.byteSize();

        long length = Math.min(firstSize, secondSize);

        final long mismatch = firstSegment.mismatch(secondSegment);
        if (mismatch >= 0 & mismatch < length) {
            return Byte.compareUnsigned((byte) BYTE_HANDLE.get(firstSegment, mismatch),
                    (byte) BYTE_HANDLE.get(secondSegment, mismatch));
        }

        return Long.compare(firstSize, secondSize);
    }
}
