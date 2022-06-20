package io.viktoriadb.operations.log;

import io.viktoriadb.exceptions.DbException;
import io.viktoriadb.exceptions.IncorrectPositionInOperationSegmentException;
import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import net.jpountz.xxhash.XXHashFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.lang.ref.WeakReference;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Presentation of single segment in operation log.
 * Segment has fixed maximum size and is append only. S
 * So once record is written it is never changed.
 * <p>
 * Each entry in segment sored in the following format:
 * xx_hash:record position:record size:record
 *
 * <ol>
 *     <li>xx-hash - consumes 8 bytes. Hash of whole content of the entry.
 *     It is used to ensure that record is not broken in segment.</li>
 *     <li>Record position - consumes 4 bytes. Position of the stored record. This field is used
 *     to ensure correctness of the position passed in {@linkplain #read(int)} method.</li>
 *     <li>Record size - consume 4 bytes. Size of the record stored </li>
 * </ol>
 */
class BaseMmapSegment implements Closeable {
    protected static final long XX_HASH_SEED = 0x420ADEF;
    protected static final XXHashFactory XX_HASH_FACTORY = XXHashFactory.fastestInstance();

    /**
     * Total size of fields containing hash code,stored record start position, record size.
     */
    protected static final int FIELDS_SIZE = Long.BYTES + 2 * Integer.BYTES;

    protected static final VarHandle LONG_HANDLE =
            MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());
    protected static final VarHandle INT_HANDLE = MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

    /**
     * Maximum size of the segment.
     */
    protected final int maximumSegmentSize;

    protected final MemorySegment segment;
    protected final ResourceScope segmentScope;
    protected final long address;


    private final Path path;

    /**
     * Queue listeners which are called once segment is closed.
     */
    private final ConcurrentLinkedQueue<WeakReference<SegmentCloseListener>> listeners =
            new ConcurrentLinkedQueue<>();

    BaseMmapSegment(int maximumSegmentSize, Path path, FileChannel.MapMode mapMode) throws IOException {
        this.maximumSegmentSize = maximumSegmentSize;
        this.path = path;

        segmentScope = ResourceScope.newSharedScope();
        segment = MemorySegment.mapFile(path, 0, maximumSegmentSize, mapMode,
                segmentScope);
        address = segment.address().toRawLongValue();
    }


    /**
     * Adds listener which is called once segment is closed.
     * Listeners are stored as {@linkplain WeakReference} and not needed to be removed.
     *
     * @param listener Instance of listener.
     */
    public final void addCloseListener(SegmentCloseListener listener) {
        listeners.add(new WeakReference<>(listener));
    }

    /**
     * Calls listeners and frees underlying allocated resources.
     *
     * @see #addCloseListener(SegmentCloseListener)
     */
    @Override
    public void close() {
        for (var listenerRef : listeners) {
            var listener = listenerRef.get();
            if (listener != null) {
                listener.onClose();
            }
        }

        segmentScope.close();
    }

    /**
     * Reads record stored in segment by passed in position.
     *
     * @param position Position of the record to read.
     * @return Instance of stored record.
     * @throws DbException if record is broken or incorrect position of record was passed.
     */
    final MemorySegment read(int position) {
        if (position + FIELDS_SIZE > maximumSegmentSize) {
            throw new DbException(String.format("Record is broken. Record with passed in position not able to " +
                            "store internal fields {position: %d, max segment size: %d, path: %s}",
                    position, maximumSegmentSize, path));
        }
        if (((address + position) & 7) != 0) {
            throw new IncorrectPositionInOperationSegmentException(String.format(
                    "Invalid record position. Passed in record position leads to misaligned memory access " +
                            "{position: %d, path: %s}",
                    position, path));
        }

        var startPosition = position;

        var storedHash = (long) LONG_HANDLE.getVolatile(segment, (long) position);
        position += Long.BYTES;

        var recordPosition = (int) INT_HANDLE.get(segment, (long) position);
        position += Integer.BYTES;

        if (recordPosition != startPosition) {
            throw new IncorrectPositionInOperationSegmentException(String.format(
                    "Invalid record position. Stored record position and passed in positions are different." +
                            "{position: %d, stored position: %d, path: %s}",
                    startPosition, recordPosition, path));
        }

        var recordSize = (int) INT_HANDLE.get(segment, (long) position);
        if (recordSize < 0 || recordSize + FIELDS_SIZE > maximumSegmentSize) {
            throw new DbException(String.format("Record is broken. Size of the record is invalid. " +
                            "{position: %d, record size : %d, max segment size: %d, path: %s}",
                    startPosition, recordSize, maximumSegmentSize, path));
        }

        var xxHash = XX_HASH_FACTORY.hash64();
        var hashChunkSize = recordSize + 2 * Integer.BYTES; //record itself and its position
        var calculatedHash = xxHash.hash(segment.asSlice(startPosition + Long.BYTES,
                        hashChunkSize).asByteBuffer(), 0,
                hashChunkSize, XX_HASH_SEED);

        if (calculatedHash != storedHash) {
            throw new DbException(String.format("Record is broken. " +
                            "Stored and calculated hash codes are different. " +
                            "{position: %d, calculated hash 0x%x, stored hash: 0x%x, path: %s}",
                    startPosition, calculatedHash, storedHash, path));
        }

        return segment.asSlice(startPosition + FIELDS_SIZE, recordSize);
    }
}
