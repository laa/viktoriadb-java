package io.viktoriadb.operations.log;

import jdk.incubator.foreign.MemorySegment;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mutable version of operations log segment.
 *
 * @see BaseMmapSegment
 */
final class MutableMmapSegment extends BaseMmapSegment {
    private final AtomicInteger currentPosition = new AtomicInteger();

    MutableMmapSegment(int segmentSize, Path path) throws IOException {
        super(segmentSize, path, FileChannel.MapMode.READ_WRITE);
    }

    /**
     * Writes record to the segment and returns position of this record inside this segment.
     * Records can not be stored if size of the record bigger than available free space.
     *
     * @param record Record to write
     * @return Positive position of the record or negative value if there is no enough free space to store record.
     */
    int log(final MemorySegment record) {
        int position;

        while (true) {
            final int recordPosition = currentPosition.get();

            //align position to write hash code and record size
            int reminder = (int) ((address + recordPosition) & 7);
            int sizeDiff = 0;

            if (reminder != 0) {
                sizeDiff = 8 - reminder;
            }

            //return if not enough space left to store record
            if (recordPosition + record.byteSize() + sizeDiff > maximumSegmentSize) {
                return -1;
            }
            if (currentPosition.compareAndSet(recordPosition, (int) (recordPosition + record.byteSize() +
                    sizeDiff + FIELDS_SIZE))) {
                position = recordPosition + sizeDiff;
                break;
            }
        }

        //skip hash code for a while
        final int startPosition = position;
        position += Long.BYTES;

        INT_HANDLE.set(segment, (long) position, startPosition);
        position += Integer.BYTES;

        INT_HANDLE.set(segment, (long) position, (int) record.byteSize());
        position += Integer.BYTES;

        segment.asSlice(position).copyFrom(record);

        var xxHash = XX_HASH_FACTORY.hash64();
        var hashChunkSize = (int) (record.byteSize() + 2 * Integer.BYTES);

        var hash = xxHash.hash(segment.asSlice(startPosition + Long.BYTES,
                        hashChunkSize).asByteBuffer(), 0,
                hashChunkSize, XX_HASH_SEED);

        LONG_HANDLE.setVolatile(segment, (long) startPosition, hash);

        return startPosition;
    }

    void flush() {
        segment.force();
    }

    @Override
    public void close() {
        flush();

        super.close();
    }
}
