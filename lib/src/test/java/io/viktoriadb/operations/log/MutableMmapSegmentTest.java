package io.viktoriadb.operations.log;

import io.viktoriadb.Utils;
import io.viktoriadb.exceptions.IncorrectPositionInOperationSegmentException;
import it.unimi.dsi.fastutil.ints.IntObjectImmutablePair;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import jdk.incubator.foreign.MemorySegment;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class MutableMmapSegmentTest {
    @Test
    public void testSingleWriteRead() throws IOException {
        var tmpPath = Files.createTempFile("vdb-test", "1");
        try {
            try (var logSegment = new MutableMmapSegment(2048, tmpPath)) {
                var data = new byte[125];
                var rnd = new Random();
                rnd.nextBytes(data);

                var record = MemorySegment.ofArray(data);
                var position = logSegment.log(record);

                Assert.assertTrue(position >= 0);

                var loadedRecord = logSegment.read(position);
                Assert.assertEquals(ByteBuffer.wrap(data), loadedRecord.asByteBuffer());
            }
        } finally {
            Utils.removeRecursively(tmpPath);
        }
    }

    @Test
    public void testWriteReadThreeRecords() throws IOException {
        var rnd = new Random();

        var tmpPath = Files.createTempFile("vdb-test", "2");
        try {
            try (var logSegment = new MutableMmapSegment(2048, tmpPath)) {
                final List<IntObjectPair<byte[]>> records = new ArrayList<>();

                for (int i = 0; i < 3; i++) {
                    var record = new byte[125 * (i + 1)];
                    rnd.nextBytes(record);

                    var position = logSegment.log(MemorySegment.ofArray(record));
                    Assert.assertTrue(position >= 0);
                    records.add(new IntObjectImmutablePair<>(position, record));
                }

                Collections.shuffle(records);

                for (int i = 0; i < 2; i++) {
                    var pair = records.get(i);
                    var record = logSegment.read(pair.firstInt());
                    Assert.assertEquals(ByteBuffer.wrap(pair.second()), record.asByteBuffer());
                }
            }
        } finally {
            Utils.removeRecursively(tmpPath);
        }
    }

    @Test
    public void testWriteTillFull() throws IOException {
        var seed = System.nanoTime();
        var rnd = new Random(seed);

        System.out.println("testWriteTillFull : seed " + seed);
        var tmpPath = Files.createTempFile("vdb-test", "3");
        try {
            try (var logSegment = new MutableMmapSegment(2048, tmpPath)) {
                final List<IntObjectPair<byte[]>> records = writeTillFull(rnd, logSegment);

                Collections.shuffle(records);

                for (var entry : records) {
                    var record = entry.second();
                    var position = entry.firstInt();

                    Assert.assertEquals(ByteBuffer.wrap(record), logSegment.read(position).asByteBuffer());
                }
            }
        } finally {
            Utils.removeRecursively(tmpPath);
        }
    }

    private List<IntObjectPair<byte[]>> writeTillFull(Random rnd, MutableMmapSegment logSegment) {
        final List<IntObjectPair<byte[]>> records = new ArrayList<>();
        while (true) {
            var recordSize = rnd.nextInt(128);
            var record = new byte[recordSize];

            rnd.nextBytes(record);
            var position = logSegment.log(MemorySegment.ofArray(record));
            if (position >= 0) {
                records.add(new IntObjectImmutablePair<>(position, record));
            } else {
                break;
            }
        }
        return records;
    }

    @Test
    public void testReadIncorrectPositionOne() throws IOException {
        var tmpPath = Files.createTempFile("vdb-test", "3");
        try {
            try (var logSegment = new MutableMmapSegment(2048, tmpPath)) {
                logSegment.log(MemorySegment.ofArray(new byte[0]));
                try {
                    logSegment.read(1);
                    Assert.fail();
                } catch (IncorrectPositionInOperationSegmentException e) {
                    //ignore
                }
            }
        } finally {
            Utils.removeRecursively(tmpPath);
        }
    }

    @Test
    public void testReadIncorrectPositionTwo() throws IOException {
        var tmpPath = Files.createTempFile("vdb-test", "3");
        try {
            try (var logSegment = new MutableMmapSegment(2048, tmpPath)) {
                var position = logSegment.log(MemorySegment.ofArray(new byte[16]));
                try {
                    logSegment.read(position + 8);
                    Assert.fail();
                } catch (IncorrectPositionInOperationSegmentException e) {
                    //ignore
                }
            }
        } finally {
            Utils.removeRecursively(tmpPath);
        }
    }
}
