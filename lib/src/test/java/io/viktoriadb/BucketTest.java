package io.viktoriadb;

import io.viktoriadb.exceptions.*;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class BucketTest extends AbstractDbTest {

    /**
     * Ensure that a bucket that gets a non-existent key returns null.
     */
    @Test
    public void testNotExistent() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            var value = bucket.get(bytes("foo"));
            Assert.assertNull(value);
        }));
    }

    /**
     * Ensure that a bucket can read a value that is not flushed yet.
     */
    @Test
    public void testFromNode() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            bucket.put(bytes("foo"), bytes("bar"));
            var value = bucket.get(bytes("foo"));
            Assert.assertEquals(bytes("bar"), value);
        }));
    }

    /**
     * Ensure that a bucket retrieved via get() returns a null.
     */
    @Test
    public void testGetIncompatibleValue() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            tx.createBucket(bytes("widgets"));

            tx.bucket(bytes("widgets")).createBucket(bytes("foo"));
            var value = tx.bucket(bytes("widgets")).get(bytes("foo"));

            Assert.assertNull(value);
        }));
    }

    /**
     * Ensure that a bucket can write a key/value.
     */
    @Test
    public void testPut() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            bucket.put(bytes("foo"), bytes("bar"));

            var value = tx.bucket(bytes("widgets")).get(bytes("foo"));
            Assert.assertEquals(bytes("bar"), value);
        }));
    }

    /**
     * Ensure that a bucket can rewrite a key in the same transaction.
     */
    @Test
    public void testPutRepeat() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            bucket.put(bytes("foo"), bytes("bar"));
            bucket.put(bytes("foo"), bytes("baz"));

            var value = tx.bucket(bytes("widgets")).get(bytes("foo"));
            Assert.assertEquals(bytes("baz"), value);
        }));
    }

    /**
     * Ensure that a bucket can write a bunch of large values.
     */
    @Test
    public void testPutLarge() {
        runTest(db -> {
            var count = 100;
            var factor = 200;

            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                for (int i = 1; i < count; i++) {
                    var key = bytes("0".repeat(i * factor));
                    var value = bytes("X".repeat((count - i) * factor));
                    bucket.put(key, value);
                }
            });

            db.executeInReadTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));

                for (int i = 1; i < count; i++) {
                    var key = bytes("0".repeat(i * factor));
                    var value = bucket.get(key);
                    Assert.assertEquals(bytes("X".repeat((count - i) * factor)), value);
                }
            });
        });
    }

    /**
     * Ensure that a database can perform multiple large appends safely.
     */
    @Test
    public void testPutVeryLarge() {
        runTest(db -> {
            var n = 400_000;
            var batch = 5_000;

            var ksize = 8;
            var vsize = 500;

            for (int i = 0; i < n; i += batch) {
                var base = i;
                db.executeInsideWriteTx(tx -> {
                    var bucket = tx.creatBucketIfNotExists(bytes("widgets"));
                    for (int j = 0; j < batch; j++) {
                        var key = ByteBuffer.allocate(ksize).putLong(0, base + j);
                        var value = ByteBuffer.allocate(vsize).putLong(0, 2L * (base + j));

                        bucket.put(key, value);
                    }
                });
            }

            db.executeInReadTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                for (int i = 0; i < n; i++) {
                    var key = ByteBuffer.allocate(ksize).putLong(0, i);
                    var value = bucket.get(key);
                    Assert.assertEquals(ByteBuffer.allocate(vsize).putLong(0, 2L * i), value);
                }
            });
        });
    }

    /**
     * Ensure that a setting a value on a key with a bucket value throws an error.
     */
    @Test
    public void testPutIncompatibleValue() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            tx.bucket(bytes("widgets")).createBucket(bytes("foo"));

            try {
                bucket.put(bytes("foo"), bytes("bar"));
                Assert.fail();
            } catch (IncompatibleValueException e) {
                //ignore
            }
        }));
    }

    /**
     * Ensure that a setting a value while the transaction is closed throws an error.
     */
    @Test
    public void testPutClosed() {
        runTest(db -> {
            var tx = db.begin(true);
            var bucket = tx.createBucket(bytes("widgets"));
            tx.rollback();

            try {
                bucket.put(bytes("foo"), bytes("bar"));
                Assert.fail();
            } catch (TransactionIsClosedException e) {
                //ignore
            }
        });
    }

    /**
     * Ensure that setting a value on a read-only bucket throws an error.
     */
    @Test
    public void testPutReadOnly() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> tx.createBucket(bytes("widgets")));
            db.executeInReadTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                try {
                    bucket.put(bytes("foo"), bytes("bar"));
                    Assert.fail();
                } catch (TransactionIsNotWritableException e) {
                    //ignore
                }
            });
        });
    }

    /**
     * Ensure that a bucket can delete an existing key.
     */
    @Test
    public void testDelete() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            bucket.put(bytes("foo"), bytes("bar"));
            bucket.delete(bytes("foo"));
            var value = bucket.get(bytes("foo"));

            Assert.assertNull(value);
        }));
    }

    /**
     * Ensure that deleting a large set of keys will work correctly.
     */
    @Test
    public void testDeleteLarge() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                for (int i = 0; i < 100; i++) {
                    bucket.put(bytes(String.valueOf(i)), bytes("*".repeat(1024)));
                }
            });

            db.executeInsideWriteTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                for (int i = 0; i < 100; i++) {
                    bucket.delete(bytes(String.valueOf(i)));
                }
            });

            db.executeInReadTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));

                for (int i = 0; i < 100; i++) {
                    var value = bucket.get(bytes(String.valueOf(i)));
                    Assert.assertNull(value);
                }
            });
        });
    }

    /**
     * Deleting a very large list of keys will cause the freelist to use overflow.
     */
    @Test
    public void testDeleteFreeListOverflow() {
        runTest(db -> {
            for (int i = 0; i < 10_000; i++) {
                var base = i;

                db.executeInsideWriteTx(tx -> {
                    var bucket = tx.creatBucketIfNotExists(bytes("widgets"));
                    for (int j = 0; j < 1_000; j++) {
                        var key = ByteBuffer.allocate(16);
                        key.putLong(base).putLong(j).rewind();

                        bucket.put(key, ByteBuffer.allocate(0));
                    }
                });
            }

            db.executeInsideWriteTx(tx -> {
                var cursor = tx.bucket(bytes("widgets")).cursor();

                var kv = cursor.first();
                while (kv != null) {
                    cursor.delete();
                    kv = cursor.next();
                }
            });

        });
    }

    /**
     * Ensure that accessing and updating nested buckets is ok across transactions.
     */
    @Test
    public void testBucketNested() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var bucket = tx.createBucket(bytes("widgets"));
                bucket.createBucket(bytes("foo"));

                bucket.put(bytes("bar"), bytes("000"));
            });

            db.executeInsideWriteTx(Tx::check);

            db.executeInsideWriteTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                bucket.put(bytes("bar"), bytes("xxxx"));
            });

            db.executeInsideWriteTx(Tx::check);

            db.executeInsideWriteTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                for (int i = 0; i < 10_000; i++) {
                    bucket.put(bytes(String.valueOf(i)), bytes(String.valueOf(2 * i)));
                }
            });

            db.executeInsideWriteTx(Tx::check);

            db.executeInsideWriteTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                var b = bucket.bucket(bytes("foo"));
                Assert.assertNotNull(b);
                b.put(bytes("baz"), bytes("yyyy"));
            });

            db.executeInsideWriteTx(Tx::check);

            db.executeInReadTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                var b = bucket.bucket(bytes("foo"));
                Assert.assertNotNull(b);

                var value = b.get(bytes("baz"));
                Assert.assertEquals(bytes("yyyy"), value);

                value = bucket.get(bytes("bar"));
                Assert.assertEquals(bytes("xxxx"), value);

                for (int i = 0; i < 10_000; i++) {
                    value = bucket.get(bytes(String.valueOf(i)));
                    Assert.assertEquals(bytes(String.valueOf(2 * i)), value);
                }
            });
        });
    }

    /**
     * Ensure that deleting a bucket using delete() throws an exception.
     */
    @Test
    public void testDeleteBucket() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            bucket.createBucket(bytes("foo"));
            try {
                bucket.delete(bytes("foo"));
                Assert.fail();
            } catch (IncompatibleValueException e) {
                //ignore
            }
        }));
    }

    /**
     * Ensure that deleting a key on a read-only bucket throws an exception.
     */
    @Test
    public void testDeleteReadOnly() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> tx.createBucket(bytes("widgets")));
            db.executeInReadTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                try {
                    bucket.delete(bytes("foo"));
                    Assert.fail();
                } catch (TransactionIsNotWritableException e) {
                    //ignore
                }
            });
        });
    }

    /**
     * Ensure that a deleting value while the transaction is closed throws an exception.
     */
    @Test
    public void testDeleteClosed() {
        runTest(db -> {
            var tx = db.begin(true);
            var bucket = tx.createBucket(bytes("widgets"));
            tx.rollback();

            try {
                bucket.delete(bytes("foo"));
                Assert.fail();
            } catch (TransactionIsClosedException e) {
                //ignore
            }
        });
    }

    /**
     * Ensure that deleting a bucket causes nested buckets to be deleted.
     */
    @Test
    public void testDeleteBucketNested() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var widgets = tx.createBucket(bytes("widgets"));
            var foo = widgets.createBucket(bytes("foo"));
            var bar = foo.createBucket(bytes("bar"));
            bar.put(bytes("baz"), bytes("bat"));
            widgets.deleteBucket(bytes("foo"));
        }));
    }

    /**
     * Ensure that deleting a bucket causes nested buckets to be deleted after they have been committed.
     */
    @Test
    public void testDeleteBucketNested2() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var widgets = tx.createBucket(bytes("widgets"));
                var foo = widgets.createBucket(bytes("foo"));
                var bar = foo.createBucket(bytes("bar"));
                bar.put(bytes("baz"), bytes("bat"));
            });

            db.executeInsideWriteTx(tx -> {
                var widgets = tx.bucket(bytes("widgets"));
                var foo = widgets.bucket(bytes("foo"));
                Assert.assertNotNull(foo);
                var bar = foo.bucket(bytes("bar"));
                Assert.assertNotNull(bar);

                var value = bar.get(bytes("baz"));
                Assert.assertEquals(bytes("bat"), value);

                tx.deleteBucket(bytes("widgets"));
            });

            db.executeInReadTx(tx -> {
                var widgets = tx.bucket(bytes("widgets"));
                Assert.assertNull(widgets);
            });
        });
    }

    /**
     * Ensure that deleting a child bucket with multiple pages causes all pages to get collected.
     */
    @Test
    public void testDeleteBucketNestedLarge() {
        runTest(db -> {
            db.executeInsideWriteTx(tx -> {
                var widgets = tx.createBucket(bytes("widgets"));
                var foo = widgets.createBucket(bytes("foo"));

                for (int i = 0; i < 1_000; i++) {
                    foo.put(bytes(String.valueOf(i)), bytes(String.valueOf(i)));
                }
            });

            db.executeInsideWriteTx(tx -> tx.deleteBucket(bytes("widgets")));
        });
    }

    /**
     * Ensure that a simple value retrieved via bucket() returns a null.
     */
    @Test
    public void testBucketIncompatibleValue() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var widgets = tx.createBucket(bytes("widgets"));
            widgets.put(bytes("foo"), bytes("bar"));
            var value = tx.bucket(bytes("widgets")).bucket(bytes("foo"));

            Assert.assertNull(value);
        }));
    }

    /**
     * Ensure that creating a bucket on an existing non-bucket key throws exception.
     */
    @Test
    public void testCreateBucketIncompatibleValue() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var widgets = tx.createBucket(bytes("widgets"));
            widgets.put(bytes("foo"), bytes("bar"));
            try {
                widgets.createBucket(bytes("foo"));
                Assert.fail();
            } catch (IncompatibleValueException e) {
                //ignore
            }
        }));
    }

    /**
     * Ensure that deleting a bucket on an existing non-bucket key throws an exception.
     */
    @Test
    public void testDeleteBucketIncompatibleValue() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var widgets = tx.createBucket(bytes("widgets"));
            widgets.put(bytes("foo"), bytes("bar"));
            try {
                tx.bucket(bytes("widgets")).deleteBucket(bytes("foo"));
                Assert.fail();
            } catch (IncompatibleValueException e) {
                //ignore
            }

        }));
    }

    /**
     * Ensure a user can loop over all key/value pairs in a bucket.
     */
    @Test
    public void testBucketForEach() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            bucket.put(bytes("foo"), bytes("0000"));
            bucket.put(bytes("baz"), bytes("0001"));
            bucket.put(bytes("bar"), bytes("0002"));

            var index = new int[1];

            bucket.forEach((k, v) -> {
                switch (index[0]) {
                    case 0 -> {
                        Assert.assertEquals(bytes("bar"), k);
                        Assert.assertEquals(bytes("0002"), v);
                    }
                    case 1 -> {
                        Assert.assertEquals(bytes("baz"), k);
                        Assert.assertEquals(bytes("0001"), v);
                    }
                    case 2 -> {
                        Assert.assertEquals(bytes("foo"), k);
                        Assert.assertEquals(bytes("0000"), v);
                    }
                    default -> throw new IllegalStateException("Invalid index");
                }

                index[0]++;
            });
        }));
    }

    /**
     * Ensure that looping over a bucket on a closed database throws an exception.
     */
    @Test
    public void testBucketForEachClosed() {
        runTest(db -> {
            var tx = db.begin(true);
            var bucket = tx.createBucket(bytes("widgets"));
            tx.rollback();

            try {
                bucket.forEach((k, v) -> {
                });
            } catch (TransactionIsClosedException e) {
                //ignore
            }
        });
    }

    /**
     * Ensure that an exception is thrown when inserting with an empty key.
     */
    @Test
    public void testEmptyKey() {
        runTest(db -> db.executeInsideWriteTx(tx -> {
            var bucket = tx.createBucket(bytes("widgets"));
            try {
                bucket.put(bytes(""), bytes("0"));
                Assert.fail();
            } catch (KeyRequiredException e) {
                //ignore
            }

            try {
                bucket.put(null, bytes("0"));
                Assert.fail();
            } catch (KeyRequiredException e) {
                //ignore
            }
        }));
    }

    /**
     * Ensure a bucket can calculate stats.
     */
    @Test
    public void testStats() {
        runTest(db -> {
            var bigValueKey = bytes("really-big-value");
            for (int i = 0; i < 500; i++) {
                var base = i;
                db.executeInsideWriteTx(tx -> {
                    var bucket = tx.creatBucketIfNotExists(bytes("widgets"));
                    bucket.put(bytes(String.format("%03d", base)), bytes(String.valueOf(base)));
                });
            }

            db.executeInsideWriteTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                bucket.put(bigValueKey, bytes("*".repeat(10_000)));
            });

            db.executeInsideWriteTx(Tx::check);

            db.executeInReadTx(tx -> {
                var bucket = tx.bucket(bytes("widgets"));
                var stats = bucket.stats();
                Assert.assertEquals(1, stats.branchPageN);
                Assert.assertEquals(0, stats.branchOverflowN);
                Assert.assertEquals(8, stats.leafPageN);
                Assert.assertEquals(2, stats.leafOverflowN);
                Assert.assertEquals(501, stats.keyN);
                Assert.assertEquals(2, stats.depthN);

                int branchInUse = BTreePage.PAGE_HEADER_SIZE; // branch page header
                branchInUse += 8 * BTreePage.BranchPageElement.SIZE; //branch page elements
                branchInUse += 7 * 3; // branch keys (6 3-byte keys)
                branchInUse += bigValueKey.remaining(); // big key size

                Assert.assertEquals(branchInUse, stats.branchInUse);

                int leafInUse = 8 * BTreePage.PAGE_HEADER_SIZE; //leaf page header size
                leafInUse += 501 * BTreePage.LeafPageElement.SIZE; // leaf elements data size
                leafInUse += 500 * 3 + bigValueKey.remaining(); //leaf keys
                //noinspection PointlessArithmeticExpression
                leafInUse += 1 * 10 + 2 * 90 + 3 * 400 + 10000; //leaf values

                Assert.assertEquals(leafInUse, stats.leafInUse);
                Assert.assertEquals(4096, stats.branchAlloc);
                Assert.assertEquals(10 * 4096, stats.leafAlloc);
                Assert.assertEquals(1, stats.bucketN);
                Assert.assertEquals(0, stats.inlineBucketN);
                Assert.assertEquals(0, stats.inlineBucketInUse);
            });
        });
    }
}
