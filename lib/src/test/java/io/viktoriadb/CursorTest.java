package io.viktoriadb;

import io.viktoriadb.exceptions.IncompatibleValueException;
import io.viktoriadb.util.ByteBufferComparator;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class CursorTest extends AbstractDbTest {
    /**
     * Ensure that a cursor can return a reference to the bucket that created it.
     */
    @Test
    public void testBucket() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            Assert.assertSame(bucket.cursor().bucket(), bucket);
        }));
    }

    /**
     * Ensure that a Tx cursor can seek to the appropriate keys.
     */
    @Test
    public void testSeek() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                bucket.put(bytes("foo"), bytes("0001"));
                bucket.put(bytes("bar"), bytes("0002"));
                bucket.put(bytes("baz"), bytes("0003"));

                bucket.createBucket(bytes("bkt"));
            });

            db.executeInReadTx(tx -> {
                var cursor = tx.bucket(bytes("widgets")).cursor();

                // Exact match should go to the key.
                var kv = cursor.seek(bytes("bar"));

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("bar"), kv[0]);
                Assert.assertEquals(bytes("0002"), kv[1]);

                // Inexact match should go to the next key.
                kv = cursor.seek(bytes("bas"));

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("baz"), kv[0]);
                Assert.assertEquals(bytes("0003"), kv[1]);

                // Low key should go to the first key.
                kv = cursor.seek(ByteBuffer.allocate(0));

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("bar"), kv[0]);
                Assert.assertEquals(bytes("0002"), kv[1]);

                // High key should return no key.
                kv = cursor.seek(bytes("zzz"));
                Assert.assertNull(kv);

                // Buckets should return their key but no value.
                kv = cursor.seek(bytes("bkt"));

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("bkt"), kv[0]);
                Assert.assertNull(kv[1]);
            });
        });
    }

    /**
     * Ensure that a Tx cursor can seek to the appropriate keys when there are a
     * large number of keys. This test also checks that seek will always move
     * forward to the next key.
     */
    @Test
    public void testSeekLarge() {
        int count = 10_000;
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                for (int i = 0; i < count; i += 100) {
                    for (int j = i; j < i + 100; j += 2) {
                        var key = ByteBuffer.allocate(8);
                        key.putLong(0, j);
                        var value = ByteBuffer.allocate(100);

                        bucket.put(key, value);
                    }
                }
            });

            db.executeInReadTx(tx -> {
                var cursor = tx.bucket(bytes("widgets")).cursor();

                for (int i = 0; i < count; i++) {
                    var key = ByteBuffer.allocate(8);
                    key.putLong(0, i);

                    var kv = cursor.seek(key);

                    // The last seek is beyond the end of the the range so
                    // it should return null.
                    if (i == count - 1) {
                        Assert.assertNull(kv);
                        continue;
                    }

                    Assert.assertNotNull(kv);
                    long num = kv[0].getLong(0);

                    byte[] bt = new byte[8];
                    kv[0].get(0, bt);
                    if (i % 2 == 0) {
                        Assert.assertEquals(i, num);
                    } else {
                        Assert.assertEquals(i + 1, num);
                    }
                }
            });
        });
    }

    /**
     * Ensure that a cursor can iterate over an empty bucket without error.
     */
    @Test
    public void testEmptyBucket() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> tx.createBucket(bytes("widgets")));

            db.executeInReadTx(tx -> {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.first();

                Assert.assertNull(kv);
            });
        });
    }

    /**
     * Ensure that a Tx cursor can reverse iterate over an empty bucket without error.
     */
    @Test
    public void testEmptyBucketReverse() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> tx.createBucket(bytes("widgets")));

            db.executeInReadTx(tx -> {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.last();

                Assert.assertNull(kv);
            });
        });
    }

    /**
     * Ensure that a Tx cursor can iterate over a single root with a couple elements.
     */
    @Test
    public void testIterateLeaf() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                bucket.put(bytes("baz"), ByteBuffer.allocate(0));
                bucket.put(bytes("foo"), ByteBuffer.allocate(1));
                bucket.put(bytes("bar"), ByteBuffer.allocate(1).put((byte) 1).rewind());
            });

            var tx = db.begin(false);
            try {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.first();

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("bar"), kv[0]);
                Assert.assertEquals(ByteBuffer.allocate(1).put((byte) 1).rewind(), kv[1]);

                kv = cursor.next();

                Assert.assertNotNull(kv);

                Assert.assertEquals(bytes("baz"), kv[0]);
                Assert.assertEquals(ByteBuffer.allocate(0), kv[1]);

                kv = cursor.next();

                Assert.assertNotNull(kv);

                Assert.assertEquals(bytes("foo"), kv[0]);
                Assert.assertEquals(ByteBuffer.allocate(1), kv[1]);

                kv = cursor.next();
                Assert.assertNull(kv);

                kv = cursor.next();
                Assert.assertNull(kv);
            } finally {
                tx.rollback();
            }
        });
    }

    /**
     * Ensure that a Tx cursor can iterate in reverse over a single root with a couple elements.
     */
    @Test
    public void testIterateLeafReverse() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));

                bucket.put(bytes("baz"), ByteBuffer.allocate(0));
                bucket.put(bytes("foo"), ByteBuffer.allocate(1));
                bucket.put(bytes("bar"), ByteBuffer.allocate(1).put((byte) 1).rewind());
            });

            var tx = db.begin(false);
            try {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.last();

                Assert.assertNotNull(kv);

                Assert.assertEquals(bytes("foo"), kv[0]);
                Assert.assertEquals(ByteBuffer.allocate(1), kv[1]);

                kv = cursor.prev();

                Assert.assertNotNull(kv);

                Assert.assertEquals(bytes("baz"), kv[0]);
                Assert.assertEquals(ByteBuffer.allocate(0), kv[1]);

                kv = cursor.prev();

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("bar"), kv[0]);
                Assert.assertEquals(ByteBuffer.allocate(1).put((byte) 1).rewind(), kv[1]);

                kv = cursor.prev();
                Assert.assertNull(kv);

                kv = cursor.prev();
                Assert.assertNull(kv);
            } finally {
                tx.rollback();
            }
        });
    }

    /**
     * Ensure that a Tx cursor can restart from the beginning.
     */
    @Test
    public void testRestart() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                bucket.put(bytes("bar"), ByteBuffer.allocate(0));
                bucket.put(bytes("foo"), ByteBuffer.allocate(0));
            });

            var tx = db.begin(false);
            try {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.first();

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("bar"), kv[0]);

                kv = cursor.next();

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("foo"), kv[0]);

                kv = cursor.first();

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("bar"), kv[0]);

                kv = cursor.next();

                Assert.assertNotNull(kv);
                Assert.assertEquals(bytes("foo"), kv[0]);
            } finally {
                tx.rollback();
            }
        });
    }

    @Test
    public void testDelete() {
        int count = 1_000;
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                for (int i = 0; i < count; i++) {
                    bucket.put(ByteBuffer.allocate(8).putLong(0, i),
                            ByteBuffer.allocate(100));
                }

                bucket.createBucket(bytes("sub"));
            });


            var bound =
                    ByteBuffer.allocate(8).putLong(count / 2).rewind();
            db.executeInsideWriteTx(tx -> {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.first();

                while (kv != null && ByteBufferComparator.INSTANCE.compare(kv[0], bound) < 0) {
                    cursor.delete();
                    kv = cursor.next();
                }

                cursor.seek(bytes("sub"));
                try {
                    cursor.delete();
                    Assert.fail();
                } catch (IncompatibleValueException e) {
                    //ignore
                }
            });

            db.executeInReadTx(tx -> {
                var stats = tx.bucket(bytes("widgets")).stats();
                Assert.assertEquals(count / 2 + 1, stats.keyN);
            });
        });
    }

    /**
     * Ensure that a cursor can skip over empty pages that have been deleted.
     */
    @Test
    public void testEmptyPages() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));

                for (int i = 0; i < 1000; i++) {
                    var key = ByteBuffer.allocate(8);
                    key.putLong(0, i);

                    bucket.put(key, ByteBuffer.allocate(0));
                }
            });

            db.executeInsideWriteTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                for (int i = 0; i < 600; i++) {
                    var key = ByteBuffer.allocate(8);
                    key.putLong(0, i);

                    bucket.delete(key);
                }

                var cursor = bucket.cursor();
                var kv = cursor.first();

                int n = 0;
                while (kv != null) {
                    kv = cursor.next();
                    n++;
                }

                Assert.assertEquals(400, n);
            });
        });
    }

    /**
     * Ensure that a Tx can iterate over all elements in a bucket.
     */
    @Test
    public void testQuickCheck() {
        var data = randomData();
        runTest(db -> {
            // Bulk insert all values.
            var tx = db.begin(true);
            try {
                var bucket = tx.createBucket(bytes("widgets"));
                for (ByteBuffer[] kv : data) {
                    bucket.put(kv[0], kv[1]);
                }
            } finally {
                tx.commit();
            }

            data.sort((bOne, bTwo) -> ByteBufferComparator.INSTANCE.compare(bOne[0], bTwo[0]));

            tx = db.begin(false);
            try {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.first();
                Assert.assertNotNull(kv);

                Assert.assertEquals(data.get(0)[0], kv[0]);
                Assert.assertEquals(data.get(0)[1], kv[1]);

                for (int i = 1; i < data.size(); i++) {
                    kv = cursor.next();

                    Assert.assertNotNull(kv);

                    Assert.assertEquals(data.get(i)[0], kv[0]);
                    Assert.assertEquals(data.get(i)[1], kv[1]);
                }

                kv = cursor.next();
                Assert.assertNull(kv);

                kv = cursor.next();
                Assert.assertNull(kv);
            } finally {
                tx.rollback();
            }
        });
    }

    /**
     * Ensure that a Tx can iterate over all elements in revrse order in a bucket.
     */
    @Test
    public void testQuickCheckReverse() {
        var data = randomData();
        runTest(db -> {
            // Bulk insert all values.
            var tx = db.begin(true);
            try {
                var bucket = tx.createBucket(bytes("widgets"));
                for (ByteBuffer[] kv : data) {
                    bucket.put(kv[0], kv[1]);
                }
            } finally {
                tx.commit();
            }

            data.sort((bOne, bTwo) -> -ByteBufferComparator.INSTANCE.compare(bOne[0], bTwo[0]));

            tx = db.begin(false);
            try {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.last();
                Assert.assertNotNull(kv);

                Assert.assertEquals(data.get(0)[0], kv[0]);
                Assert.assertEquals(data.get(0)[1], kv[1]);

                for (int i = 1; i < data.size(); i++) {
                    kv = cursor.prev();

                    Assert.assertNotNull(kv);

                    Assert.assertEquals(data.get(i)[0], kv[0]);
                    Assert.assertEquals(data.get(i)[1], kv[1]);
                }

                kv = cursor.prev();
                Assert.assertNull(kv);

                kv = cursor.prev();
                Assert.assertNull(kv);
            } finally {
                tx.rollback();
            }
        });
    }

    /**
     * Ensure that a Tx cursor can iterate over subbuckets.
     */
    @Test
    public void testQuickCheckBucketsOnly() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                bucket.createBucket(bytes("foo"));
                bucket.createBucket(bytes("bar"));
                bucket.createBucket(bytes("baz"));
            });

            db.executeInReadTx(tx -> {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.first();
                ArrayList<String> buckets = new ArrayList<>();


                while (kv != null) {
                    Assert.assertNotNull(kv);
                    Assert.assertNull(kv[1]);


                    buckets.add(string(kv[0]));
                    kv = cursor.next();
                }

                Assert.assertEquals(Arrays.asList("bar", "baz", "foo"), buckets);
            });
        });
    }

    /**
     * Ensure that a Tx cursor can iterate over subbuckets in a reverse way.
     */
    @Test
    public void testQuickCheckBucketsOnlyReverse() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                bucket.createBucket(bytes("foo"));
                bucket.createBucket(bytes("bar"));
                bucket.createBucket(bytes("baz"));
            });

            db.executeInReadTx(tx -> {
                var cursor = tx.bucket(bytes("widgets")).cursor();
                var kv = cursor.last();
                ArrayList<String> buckets = new ArrayList<>();


                while (kv != null) {
                    Assert.assertNotNull(kv);
                    Assert.assertNull(kv[1]);


                    buckets.add(string(kv[0]));
                    kv = cursor.prev();
                }

                Assert.assertEquals(Arrays.asList("foo", "baz", "bar"), buckets);
            });
        });
    }

}
