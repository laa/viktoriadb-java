package io.viktoriadb.util;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class ByteBufferComparator implements Comparator<ByteBuffer> {
    public static final ByteBufferComparator INSTANCE = new ByteBufferComparator();

    @Override
    public int compare(final ByteBuffer firstBuffer, final ByteBuffer secondBuffer) {
        int firstPos = firstBuffer.position();
        int firstRem = firstBuffer.limit() - firstPos;
        int secondPos = secondBuffer.position();
        int secondRem = secondBuffer.limit() - secondPos;
        int length = Math.min(firstRem, secondRem);

        final int mismatch = firstBuffer.mismatch(secondBuffer);
        if (mismatch >= 0 & mismatch < length) {
            return Byte.compareUnsigned(firstBuffer.get(mismatch + firstPos),
                    secondBuffer.get(mismatch + secondPos));
        }

        return firstRem - secondRem;
    }
}
