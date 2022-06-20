package io.viktoriadb.operations.log;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * Read only version of operations log segment.
 *
 * @sed BaseMmapSegment
 * @see MutableMmapSegment
 */
public class MmapSegment extends BaseMmapSegment {
    MmapSegment(int maximumSegmentSize, Path path, FileChannel.MapMode mapMode) throws IOException {
        super(maximumSegmentSize, path, mapMode);
    }
}
