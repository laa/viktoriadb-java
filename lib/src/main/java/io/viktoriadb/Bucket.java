package io.viktoriadb;

import io.viktoriadb.exceptions.*;
import io.viktoriadb.util.MemorySegmentComparator;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;
import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemorySegment;

import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;

/**
 * Represents a collection of key/value pairs inside the database.
 */
public final class Bucket {
    private static final VarHandle ROOT_VAR_HANDLE = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());

    /**
     * Maximum length of a key, in bytes.
     */
    public static final int MAX_KEY_SIZE = 32768;

    /**
     * Maximum length of a value, in bytes.
     */
    public static final int MAX_VALUE_SIZE = (1 << 31) - 2;

    /**
     * Default percentage that split pages are filled.
     */
    static final float DEFAULT_FILL_PERCENT = 0.5f;

    static final int BUCKET_LEAF_FLAG = 0x01;
    static final float MIN_FILL_PERCENT = 0.1f;
    static final float MAX_FILL_PERCENT = 1.0f;

    /**
     * Associated transaction
     */
    Tx tx;

    /**
     * Tree root associated with the bucket
     */
    long root;

    /**
     * Threshold for filling nodes when they split. By default,
     * the bucket will fill to 50% but it can be useful to increase this
     * amount if you know that your write workloads are mostly append-only.
     * <p>
     * This is non-persisted across transactions so it must be set in every Tx.
     */
    float fillPercent;

    /**
     * Nodes cache
     */
    Long2ObjectOpenHashMap<Node> nodes;

    /**
     * Subbucket cache
     */
    HashMap<ByteBuffer, Bucket> buckets;

    /**
     * Inline page reference.
     */
    BTreePage page;

    /**
     * Materialized node for the root page.
     */
    Node rootNode;

    boolean isRootBucket;

    /**
     * Presentation of bucket inside of the disk.
     * This value is filled only after the call of {@link #spill()} method. So it is written only once.
     */
    MemorySegment spilled;

    Bucket(Tx tx, boolean isRootBucket) {
        this.tx = tx;
        this.fillPercent = DEFAULT_FILL_PERCENT;
        this.isRootBucket = isRootBucket;

        if (tx.writable) {
            buckets = new HashMap<>();
            nodes = new Long2ObjectOpenHashMap<>();
        }
    }

    /**
     * Sets threshold for filling nodes when they split. By default,
     * the bucket will fill to 50% but it can be useful to increase this
     * amount if you know that your write workloads are mostly append-only.
     * <p>
     * This is non-persisted across transactions so it must be set in every Tx.
     *
     * @param fillPercent threshold for filling nodes when they split.
     */
    @SuppressWarnings("unused")
    public void setFillPercent(float fillPercent) {
        this.fillPercent = fillPercent;
    }

    /**
     * @return the tx of the bucket.
     */
    public Tx tx() {
        return tx;
    }

    /**
     * @return Root of the bucket.
     */
    public long root() {
        return root;
    }

    /**
     * @return whether the bucket is writable.
     */
    public boolean writable() {
        return tx.writable;
    }

    /**
     * Creates a cursor associated with the bucket.
     * The cursor is only valid as long as the transaction is open.
     * Do not use a cursor after the transaction is closed.
     *
     * @return a cursor associated with the bucket.
     */
    public Cursor cursor() {
        // Update transaction statistics.
        tx.stats.cursorCount++;

        return new Cursor(this);
    }

    /**
     * Retrieves a nested bucket by name.
     * The bucket instance is only valid for the lifetime of the transaction.
     *
     * @param name Bucket name.
     * @return nested bucket instance of null if such bucket does not exist.
     */
    public Bucket bucket(ByteBuffer name) {
        if (buckets != null) {
            var child = buckets.get(name);
            if (child != null) {
                return child;
            }
        } else {
            //we cache buckets after the write so they can be reused in read-only transactions.
            //buckets are not null only during write transactions
            if (isRootBucket) {
                var bucket = tx.db.buckets.get(name);

                if (bucket != null) {
                    return openBucket(bucket);
                }
            }
        }

        var nameSegment = MemorySegment.ofByteBuffer(name);
        var cursor = cursor();

        var kvFlags = cursor.doSeek(nameSegment);
        if (kvFlags == null) {
            return null;
        }

        // Return null if the key doesn't exist or it is not a bucket.
        if (MemorySegmentComparator.INSTANCE.compare(kvFlags.first, nameSegment) != 0
                || (kvFlags.third & BUCKET_LEAF_FLAG) == 0) {
            return null;
        }

        // Otherwise create a bucket and cache it.
        var child = openBucket(kvFlags.second);
        if (buckets != null) {
            buckets.put(unmap(kvFlags.first), child);
        }

        return child;
    }

    /**
     * Helper method that re-interprets a sub-bucket value.
     * from a parent into a Bucket.
     *
     * @param bucketValue Serialized presentation of bucket.
     * @return Deserialized presentation of {@link  Bucket}.
     */
    private Bucket openBucket(MemorySegment bucketValue) {
        var child = new Bucket(tx, false);

        if (bucketValue.isNative() && (bucketValue.address().toRawLongValue() & 7) != 0) {
            var heapSegment = MemorySegment.ofArray(new byte[(int) bucketValue.byteSize()]);
            heapSegment.copyFrom(bucketValue);

            bucketValue = heapSegment;
        }

        child.root = (long) ROOT_VAR_HANDLE.get(bucketValue, 0);

        //Save a reference to the inline page if the bucket is inline.
        if (child.root == 0) {
            var memorySegment = bucketValue.asSlice(Long.BYTES);
            //if alignment of bucket is broken
            child.page = new BTreePage(memorySegment);
        }

        return child;
    }

    /**
     * Creates a new bucket at the given key and returns the new bucket.
     * The bucket instance is only valid for the lifetime of the transaction.
     *
     * @param name Bucket name
     * @return Newly created bucket.
     * @throws SecurityException if the key already exists, if the bucket name is blank,
     *                           or if the bucket name is too long.
     */
    public Bucket createBucket(ByteBuffer name) {
        if (tx.db == null) {
            throw new TransactionIsClosedException();
        } else if (!tx.writable) {
            throw new TransactionIsNotWritableException();
        } else if (name == null || name.remaining() == 0) {
            throw new BucketNameRequiredException();
        }

        var segmentKey = MemorySegment.ofArray(new byte[name.remaining()]);
        segmentKey.asByteBuffer().put(0, name, 0, name.remaining());


        var cursor = cursor();
        var kvFlags = cursor.doSeek(segmentKey);

        if (kvFlags != null && MemorySegmentComparator.INSTANCE.compare(kvFlags.first, segmentKey) == 0) {
            if ((kvFlags.third & BUCKET_LEAF_FLAG) != 0) {
                throw new BucketAlreadyExistException();
            }

            throw new IncompatibleValueException();
        }

        // Create empty, inline bucket.
        var bucket = new Bucket(tx, false);
        bucket.rootNode = new Node();
        bucket.rootNode.isLeaf = true;

        var value = bucket.write();
        cursor.node().put(segmentKey, segmentKey, value, 0, BUCKET_LEAF_FLAG);

        // Since subbuckets are not allowed on inline buckets, we need to
        // dereference the inline page, if it exists. This will cause the bucket
        // to be treated as a regular, non-inline bucket for the rest of the tx.
        page = null;

        return bucket(name);
    }

    /**
     * Creates a new bucket if it doesn't already exist and returns a reference to it.
     * The bucket instance is only valid for the lifetime of the transaction.
     *
     * @param name Bucket name.
     * @return Newly created or existing bucket.
     * @throws DbException if the bucket name is blank, or if the bucket name is too long.
     */
    public Bucket createBucketIfNotExists(ByteBuffer name) {
        Bucket bucket;
        try {
            bucket = createBucket(name);
        } catch (BucketAlreadyExistException e) {
            return bucket(name);
        }

        return bucket;
    }

    /**
     * Deletes a bucket at the given key.
     *
     * @param name Bucket name.
     * @throws DbException if the bucket does not exist, or if the key represents a non-bucket value.
     */
    public void deleteBucket(ByteBuffer name) {
        if (tx.db == null) {
            throw new TransactionIsClosedException();
        } else if (!tx.writable) {
            throw new TransactionIsNotWritableException();
        }

        var segmentName = MemorySegment.ofByteBuffer(name);

        var cursor = cursor();
        var kvFlags = cursor.doSeek(segmentName);
        if (kvFlags == null) {
            throw new BucketNotFoundException();
        }
        if (MemorySegmentComparator.INSTANCE.compare(kvFlags.first, segmentName) != 0) {
            throw new BucketNotFoundException();
        }
        if ((kvFlags.third & BUCKET_LEAF_FLAG) == 0) {
            throw new IncompatibleValueException();
        }

        var child = bucket(name);
        Objects.requireNonNull(child);

        // Recursively delete all child buckets.
        child.forEach((k, v) -> {
            //only bucket entries has null values.
            if (v == null) {
                child.deleteBucket(k);
            }
        });

        buckets.remove(name);

        tx.db.buckets.remove(name);

        child.nodes = null;
        child.rootNode = null;
        child.free();

        // Delete the node if we have a matching key.
        cursor.node().del(segmentName);
    }

    /**
     * Retrieves the value for a key in the bucket.
     * The returned value is only valid for the life of the transaction.
     *
     * @param key key to be searched.
     * @return a null value if the key does not exist or if the key is a nested bucket.
     */
    public ByteBuffer get(ByteBuffer key) {
        var segmentKey = MemorySegment.ofByteBuffer(key);

        var kvFlags = cursor().doSeek(segmentKey);

        if (kvFlags == null) {
            return null;
        }

        if ((kvFlags.third & BUCKET_LEAF_FLAG) != 0) {
            return null;
        }

        if (MemorySegmentComparator.INSTANCE.compare(segmentKey, kvFlags.first) != 0) {
            return null;
        }

        return unmap(kvFlags.second).asReadOnlyBuffer();
    }

    /**
     * Insert  value and a key in the bucket.
     * If the key exist then its previous value will be overwritten.
     *
     * @param key   Key to insert.
     * @param value Value to insert.
     * @throws DbException if the bucket was created from a read-only transaction,
     *                     if the key is blank, if the key is too large, or if the value is too large.
     */
    public void put(ByteBuffer key, ByteBuffer value) {
        if (key == null) {
            throw new KeyRequiredException();
        }
        if (key.remaining() == 0) {
            throw new KeyRequiredException();
        }
        if (key.remaining() > MAX_KEY_SIZE) {
            throw new KeyTooLargeException();
        }
        if (value.remaining() > MAX_VALUE_SIZE) {
            throw new ValueTooLargeException();
        }


        var segmentKey = MemorySegment.ofArray(new byte[key.remaining()]);
        segmentKey.asByteBuffer().put(0, key, 0, key.remaining());

        var segmentValue = MemorySegment.ofArray(new byte[value.remaining()]);
        segmentValue.asByteBuffer().put(0, value, 0, value.remaining());

        if (tx.db == null) {
            throw new TransactionIsClosedException();
        }

        if (!tx.writable) {
            throw new TransactionIsNotWritableException();
        }

        var cursor = cursor();
        var kvFlags = cursor.doSeek(segmentKey);

        // Return an error if there is already existing bucket value.
        if (kvFlags != null && (kvFlags.third & BUCKET_LEAF_FLAG) != 0 &&
                MemorySegmentComparator.INSTANCE.compare(kvFlags.first, segmentKey) == 0) {
            throw new IncompatibleValueException();
        }

        // Delete the node if we have a matching key.
        cursor.node().put(segmentKey, segmentKey, segmentValue, 0, 0);
    }

    /**
     * Removes a key from the bucket.
     * If the key does not exist then nothing is done.
     *
     * @param key Key to remove
     * @throws DbException if the bucket was created from a read-only transaction.
     */
    public void delete(ByteBuffer key) {
        if (tx.db == null) {
            throw new TransactionIsClosedException();
        }

        if (!tx.writable) {
            throw new TransactionIsNotWritableException();
        }

        var segmentKey = MemorySegment.ofByteBuffer(key);

        // Move cursor to correct position.
        var cursor = cursor();
        var kvFlags = cursor.doSeek(segmentKey);
        if (kvFlags == null) {
            return;
        }

        if ((kvFlags.third & BUCKET_LEAF_FLAG) != 0) {
            throw new IncompatibleValueException();
        }

        // Delete the node if we have a matching key.
        cursor.node().del(segmentKey);
    }

    /**
     * Writes all the nodes for this bucket to dirty pages.
     */
    public void spill() {
        // Spill all child buckets first.
        for (var entry : buckets.entrySet()) {
            // If the child bucket is small enough and it has no child buckets then
            // write it inline into the parent bucket's page. Otherwise spill it
            // like a normal bucket and make the parent value a pointer to the page.

            MemorySegment value;
            var child = entry.getValue();
            if (child.inlinable()) {
                child.free();
                value = child.write();
            } else {
                child.spill();

                //Use the value of child root
                value = MemorySegment.ofArray(new byte[Long.BYTES]);
                ROOT_VAR_HANDLE.set(value, 0, child.root);
            }

            // Skip writing the bucket if there are no materialized nodes.
            if (child.rootNode == null) {
                continue;
            }

            child.spilled = value;

            // Update parent node.
            var cursor = cursor();
            var bucketName = MemorySegment.ofByteBuffer(entry.getKey());
            var kvFlags = cursor.doSeek(bucketName);

            if (kvFlags == null || MemorySegmentComparator.INSTANCE.compare(kvFlags.first, bucketName) != 0) {
                throw new DbException("misplaced bucket header");
            }
            if ((kvFlags.third & BUCKET_LEAF_FLAG) == 0) {
                throw new DbException(String.format("unexpected bucket header flag: %x", kvFlags.third));
            }

            cursor.node().put(bucketName, bucketName, value, 0, BUCKET_LEAF_FLAG);
        }

        // Ignore if there's not a materialized root node.
        if (rootNode == null) {
            return;
        }

        // Spill nodes.
        rootNode.spill();
        //If root node is split
        rootNode = rootNode.root();

        if (rootNode.pageId > tx.meta.getMaxPageId()) {
            throw new DbException(
                    String.format("pgid (%d) above high water mark (%d)", rootNode.pageId, tx.meta.getMaxPageId()));
        }

        root = rootNode.pageId;
    }

    /**
     * Attempts to balance all nodes.
     */
    void rebalance() {
        ArrayList<Node> nodesToBalance = new ArrayList<>();

        for (var node : nodes.values()) {
            if (node.unbalanced) {
                nodesToBalance.add(node);
            }
        }

        for (var node : nodesToBalance) {
            node.rebalance();
        }

        for (var child : buckets.values()) {
            child.rebalance();
        }
    }

    /**
     * @return true if a bucket is small enough to be written inline and if it contains no subbuckets.
     * Otherwise false.
     */
    private boolean inlinable() {
        // Bucket must only contain a single leaf node.
        if (rootNode == null || !rootNode.isLeaf) {
            return false;
        }

        // Bucket is not inlineable if it contains subbuckets or if it goes beyond
        // our threshold for inline bucket size.
        var size = Long.SIZE;
        var maxInlineBucketSize = maxInlineBucketSize();
        for (var inode : rootNode.inodes) {
            if ((inode.flags & BUCKET_LEAF_FLAG) != 0) {
                return false;
            }

            size += BTreePage.LeafPageElement.SIZE + (int) inode.value.byteSize() + (int) inode.key.byteSize();
            if (size > maxInlineBucketSize) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return maximum total size of a bucket to make it a candidate for inlining.
     */
    private int maxInlineBucketSize() {
        return tx.db.pageSize / 4;
    }


    /**
     * Recursively frees all pages in the bucket.
     */
    void free() {
        //inline bucket, nothing to free
        if (root == 0) {
            return;
        }

        var tx = this.tx;
        forEachPageNode((page, node) -> {
            if (page != null) {
                tx.db.freeList.free(tx.meta.getTXId(), page);
            } else {
                node.free();
            }
        });

        root = 0;
    }

    /**
     * Iterates over every page (or node) in a bucket.
     * This also includes inline pages.
     *
     * @param consumer function to execute on each node/page.
     */
    private void forEachPageNode(BiConsumer<BTreePage, Node> consumer) {
        // If we have an inline page then just use that.
        if (page != null) {
            consumer.accept(page, null);
        }

        forEachPageNodeRec(root, consumer);
    }

    private void forEachPageNodeRec(long pageId, BiConsumer<BTreePage, Node> consumer) {
        var pageNode = pageNode(pageId);
        consumer.accept(pageNode.first(), pageNode.second());

        // Recursively loop over children.
        var page = pageNode.first();
        if (page != null) {
            if ((page.getFlags() & Page.BRANCH_PAGE_FLAG) != 0) {
                for (int i = 0; i < page.getCount(); i++) {
                    var elem = page.getBranchElement(i);
                    forEachPageNodeRec(elem.getPageId(), consumer);
                }
            }
        } else {
            var node = pageNode.second();

            if (!node.isLeaf) {
                for (var inode : node.inodes) {
                    forEachPageNodeRec(inode.pageId, consumer);
                }
            }
        }
    }

    /**
     * Executes a function for each key/value pair in a bucket.
     * The provided function must not modify
     * the bucket; this will result in undefined behavior.
     *
     * @param consumer Function to execute.
     */
    public void forEach(BiConsumer<ByteBuffer, ByteBuffer> consumer) {
        if (tx.db == null) {
            throw new TransactionIsClosedException();
        }

        var cursor = cursor();
        var kv = cursor.first();
        while (kv != null) {
            consumer.accept(kv[0], kv[1]);
            kv = cursor.next();
        }
    }


    /**
     * Allocates and writes a bucket to a byte slice.
     * This method should be called only for inline buckets.
     *
     * @return serialized presentation of bucket.
     */
    MemorySegment write() {
        // Allocate the appropriate size.
        var value = MemorySegment.ofArray(new byte[Long.BYTES + rootNode.size()]);
        ROOT_VAR_HANDLE.set(value, 0, root);

        rootNode.write(new BTreePage(value.asSlice(Long.BYTES)));

        return value;
    }

    /**
     * @param pageId Page ID
     * @return the in-memory node, if it exists. Otherwise returns the underlying page.
     */
    Pair<BTreePage, Node> pageNode(long pageId) {
        // Inline buckets have a fake page embedded in their value so treat them
        // differently. We'll return the rootNode (if available) or the fake page.
        if (root == 0) {
            if (pageId != 0) {
                throw new DbException(String.format("inline bucket non-zero page access(2): %d != 0", pageId));
            }

            if (rootNode != null) {
                return new ObjectObjectImmutablePair<>(null, rootNode);
            }

            return new ObjectObjectImmutablePair<>(page, null);
        }

        // Check the node cache for non-inline buckets.
        if (nodes != null) {
            var node = nodes.get(pageId);
            if (node != null) {
                return new ObjectObjectImmutablePair<>(null, node);
            }
        }

        // Finally lookup the page from the transaction if no node is materialized.
        return new ObjectObjectImmutablePair<>((BTreePage) tx.page(pageId), null);
    }

    /**
     * Creates a node from a page and associates it with a given parent.
     *
     * @param pageId ID of the page
     * @param parent Parent which will be associated with loaded page.
     * @return Newly created and associated node.
     */
    Node node(long pageId, Node parent) {
        assert nodes != null : "nodes map expected";

        // Retrieve node if it's already been created.
        var node = nodes.get(pageId);
        if (node != null) {
            return node;
        }

        // Otherwise create a node and cache it.
        node = new Node();
        node.bucket = this;
        node.parent = parent;

        if (parent == null) {
            rootNode = node;
        } else {
            parent.children.add(node);
        }

        // Use the inline page if this is an inline bucket.
        var page = this.page;
        if (page == null) {
            page = (BTreePage) tx.page(pageId);
        }

        // Read the page into the node and cache it.
        node.read(page);
        nodes.put(pageId, node);

        tx.stats.nodeCount++;

        return node;
    }

    /**
     * Removes all references to the old mmap.
     */
    void dereference() {
        if (rootNode != null) {
            rootNode.dereference();
        }

        for (var child : buckets.values()) {
            child.dereference();
        }
    }

    /**
     * @return stats on a bucket
     */
    public BucketStats stats() {
        var stats = new BucketStats();
        var subStats = new BucketStats();

        var pageSize = tx.db.pageSize;
        stats.bucketN = 1;
        if (root == 0) {
            stats.inlineBucketN = 1;
        }

        forEachPage((page, depth) -> {
            if ((page.getFlags() & Page.LEAF_PAGE_FLAG) != 0) {
                stats.keyN += page.getCount();

                // used totals the used bytes for the page
                var used = BTreePage.PAGE_HEADER_SIZE;
                if (page.getCount() != 0) {
                    // If page has any elements, add all element headers.
                    used += BTreePage.LeafPageElement.SIZE * (page.getCount() - 1);

                    // Add all element key, value sizes.
                    // The computation takes advantage of the fact that the position
                    // of the last element's key/value equals to the total of the sizes
                    // of all previous elements' keys and values.
                    // It also includes the last element's header.
                    var lastElement = page.getLeafElement(page.getCount() - 1);
                    used += lastElement.getPos() + lastElement.getKSize() + lastElement.getVSize();
                }

                if (root == 0) {
                    stats.inlineBucketInUse += used;
                } else {
                    // For non-inlined bucket update all the leaf stats
                    stats.leafPageN++;
                    stats.leafInUse += used;
                    stats.leafOverflowN += page.getOverflow();
                }

                // Collect stats from sub-buckets.
                // Do that by iterating over all element headers
                // looking for the ones with the bucketLeafFlag.
                for (int i = 0; i < page.getCount(); i++) {
                    var elem = page.getLeafElement(i);
                    if ((elem.getFlags() & BUCKET_LEAF_FLAG) != 0) {
                        // For any bucket element, open the element value
                        // and recursively call Stats on the contained bucket.
                        subStats.add(openBucket(elem.getValue()).stats());
                    }
                }
            } else if ((page.getFlags() & Page.BRANCH_PAGE_FLAG) != 0) {
                stats.branchPageN++;

                // used totals the used bytes for the page
                // Add header and all element headers.
                var used = BTreePage.PAGE_HEADER_SIZE + BTreePage.BranchPageElement.SIZE * (page.getCount() - 1);

                var lastElement = page.getBranchElement(page.getCount() - 1);

                // Add size of all keys and values.
                // Again, use the fact that last element's position equals to
                // the total of key, value sizes of all previous elements.
                used += lastElement.getPos() + lastElement.getKSize();

                stats.branchInUse += used;
                stats.branchOverflowN += page.getOverflow();

            }

            // Keep track of maximum page depth.
            if (depth + 1 > stats.depthN) {
                stats.depthN = depth + 1;
            }
        });

        // Alloc stats can be computed from page counts and pageSize.
        stats.branchAlloc = (stats.branchPageN + stats.branchOverflowN) * (long) pageSize;
        stats.leafAlloc = (stats.leafPageN + stats.leafOverflowN) * (long) pageSize;

        // Add the max depth of sub-buckets to get total nested depth.
        stats.depthN += subStats.depthN;
        // Add the stats for all sub-buckets
        stats.add(subStats);

        return stats;
    }

    /**
     * Iterates over every page in a bucket, including inline pages.
     *
     * @param consumer Consumer to execute. Accepts instance of page and dept of iteration as parameters.
     */
    private void forEachPage(ObjIntConsumer<BTreePage> consumer) {
        // If we have an inline page then just use that.
        if (page != null) {
            consumer.accept(page, 0);
            return;
        }

        tx.forEachPage(root, 0, consumer);
    }

    /**
     * If buffer is mapped by mmap and current tx is writable tx then it could be invalidated during remmaping.
     * To prevent this buffer is copied.
     *
     * @param memorySegment Buffer to copy
     * @return New buffer instance with the same data if needed.
     */
    private ByteBuffer unmap(MemorySegment memorySegment) {
        if (tx.writable && memorySegment.isNative()) {
            var segment = MemorySegment.ofArray(new byte[(int) memorySegment.byteSize()]);
            segment.copyFrom(memorySegment);

            return segment.asByteBuffer();
        }

        return memorySegment.asByteBuffer();
    }

    /**
     * Records statistics about resources used by a bucket.
     */
    public static final class BucketStats {
        // Page count statistics.
        /**
         * Number of logical branch pages
         */
        int branchPageN;

        /**
         * Number of physical branch overflow pages
         */
        int branchOverflowN;

        /**
         * Number of logical leaf pages
         */
        int leafPageN;

        /**
         * Number of physical leaf overflow pages
         */
        int leafOverflowN;

        // Tree statistics.
        /**
         * Number of keys/value pairs
         */
        int keyN;
        /**
         * Number of levels in B+tree
         */
        int depthN;

        // Page size utilization.
        /**
         * bytes allocated for physical branch pages
         */
        long branchAlloc;

        /**
         * bytes actually used for branch data
         */
        long branchInUse;

        /**
         * bytes allocated for physical leaf pages
         */
        long leafAlloc;

        /**
         * bytes actually used for leaf data
         */
        long leafInUse;

        // Bucket statistics
        /**
         * total number of buckets including the top bucket
         */
        int bucketN;

        /**
         * total number on inlined buckets
         */
        int inlineBucketN;

        /**
         * bytes used for inlined buckets (also accounted for in LeafInuse)
         */
        long inlineBucketInUse;

        public void add(BucketStats other) {
            branchPageN += other.branchPageN;
            branchOverflowN += other.branchOverflowN;
            leafPageN += other.leafPageN;
            leafOverflowN += other.leafOverflowN;
            keyN += other.keyN;

            if (depthN < other.depthN) {
                depthN = other.depthN;
            }

            branchAlloc += other.branchAlloc;
            branchInUse += other.branchInUse;
            leafAlloc += other.leafAlloc;
            leafInUse += other.leafInUse;

            bucketN += other.bucketN;
            inlineBucketN += other.inlineBucketN;
            inlineBucketInUse += other.inlineBucketInUse;
        }

    }
}

