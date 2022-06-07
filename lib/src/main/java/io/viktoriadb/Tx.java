package io.viktoriadb;

import io.viktoriadb.exceptions.DbException;
import io.viktoriadb.exceptions.TransactionIsClosedException;
import io.viktoriadb.exceptions.TransactionIsNotWritableException;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;

/**
 * Tx represents a read-only or read/write transaction on the database.
 * Read-only transactions can be used for retrieving values for keys and creating cursors.
 * Read/write transactions can create and remove buckets and create and remove keys.
 * <p>
 * IMPORTANT: You must commit or rollback transactions when you are done with
 * them. Pages can not be reclaimed by the writer until no more transactions
 * are using them. A long running read transaction can cause the database to
 * quickly grow.
 */
@SuppressWarnings("unused")
public final class Tx {
    DB db;
    boolean writable;
    boolean managed;
    Bucket root;

    Meta meta;
    TxStats stats = new TxStats();

    Long2ObjectOpenHashMap<Page> dirtyPages;

    ArrayList<Runnable> commitHandlers = new ArrayList<>();

    ResourceScope scope;

    Tx() {
    }

    Tx(DB db, boolean writable) {
        this.db = db;
        this.writable = writable;

        // Add the stats for all sub-buckets

        var metaSegment = MemorySegment.ofArray(new byte[(int) Meta.LAYOUT.byteSize()]);
        metaSegment.copyFrom(db.meta().pageSegment.asSlice(0, metaSegment.byteSize()));

        this.meta = new Meta(metaSegment);

        this.root = new Bucket(this, true);
        this.root.root = this.meta.getRoot();

        // Increment the transaction id and add a page cache for writable transactions.
        if (writable) {
            this.dirtyPages = new Long2ObjectOpenHashMap<>();
            this.meta.setTXId(this.meta.getTXId() + 1);

            this.scope = ResourceScope.newSharedScope();
        }
    }

    /**
     * @return the transaction id.
     */
    public long id() {
        return meta.getTXId();
    }

    /**
     * @return reference to the database that created the transaction.
     */
    public DB db() {
        return db;
    }

    /**
     * @return current database size in bytes as seen by this transaction.
     */
    public long size() {
        return meta.getMaxPageId() * db.pageSize;
    }

    /**
     * @return whether the transaction can perform write operations.
     */
    public boolean writable() {
        return writable;
    }

    /**
     * Creates a cursor associated with the root bucket.
     * All items in the cursor will return a null value because all root bucket keys point to buckets.
     * The cursor is only valid as long as the transaction is open.
     * Do not use a cursor after the transaction is closed.
     *
     * @return Do not use a cursor after the transaction is closed.
     */
    public Cursor cursor() {
        return root.cursor();
    }

    /**
     * @return current transaction statistics
     */
    public TxStats stats() {
        return stats;
    }

    /**
     * Retrieves a bucket by name.
     * Returns null if the bucket does not exist.
     * The bucket instance is only valid for the lifetime of the transaction.
     *
     * @param name Bucket name.
     * @return Bucket instance.
     */
    public Bucket bucket(ByteBuffer name) {
        return root.bucket(name);
    }

    /**
     * Creates a new bucket.
     * The bucket instance is only valid for the lifetime of the transaction.
     *
     * @param name Bucket name.
     * @return Instance of newly created bucket.
     * @throws DbException if the bucket already exists, if the bucket name is blank,
     *                     or if the bucket name is too long.
     */
    public Bucket createBucket(ByteBuffer name) {
        return root.createBucket(name);
    }

    /**
     * Creates a new bucket if it doesn't already exist.
     * The bucket instance is only valid for the lifetime of the transaction.
     *
     * @param name Bucket name
     * @return Bucket instance.
     * @throws DbException an error if the bucket name is blank,
     *                     or if the bucket name is too long.
     */
    public Bucket creatBucketIfNotExists(final ByteBuffer name) {
        return root.createBucketIfNotExists(name);
    }

    /**
     * Deletes a bucket.
     *
     * @param name Bucket name.
     * @throws DbException if the bucket cannot be found or
     *                     if the key represents a non-bucket value.
     */
    public void deleteBucket(ByteBuffer name) {
        root.deleteBucket(name);
    }

    /**
     * Executes a function for each bucket in the root.
     *
     * @param consumer cals function on each bucket. As parameter it accepts bucket name and bucket instance.
     */
    public void forEach(BiConsumer<ByteBuffer, Bucket> consumer) {
        root.forEach((k, v) -> consumer.accept(k, root.bucket(k)));
    }

    /**
     * Adds a handler function to be executed after the transaction successfully commits.
     *
     * @param commitHandler commit handler instance.
     */
    public void addOnCommitHandler(Runnable commitHandler) {
        commitHandlers.add(commitHandler);
    }

    /**
     * Writes all changes to disk and updates the meta page.
     *
     * @throws DbException if a disk write error occurs, or if commit is
     *                     called on a read-only transaction.
     */
    public void commit() {
        assert !managed : "Managed commit is not allowed";
        if (db == null) {
            throw new TransactionIsClosedException();
        }
        if (!writable) {
            throw new TransactionIsNotWritableException();
        }

        try {
            var startTime = System.nanoTime();
            root.rebalance();
            if (stats.rebalance > 0) {
                stats.rebalanceTime = System.nanoTime() - startTime;
            }

            var oldMaxPageId = meta.getMaxPageId();
            // spill data onto dirty pages.
            startTime = System.nanoTime();
            root.spill();
            stats.spillTime = System.nanoTime() - startTime;

            // Free the old root bucket.
            meta.setRoot(root.root);

            // Free the freelist and allocate new pages for it. This will overestimate
            // the size of the freelist but not underestimate the size (which would be bad).
            db.freeList.free(meta.getTXId(), db.page(meta.getFreeList()));
            var page = allocate((db.freeList.size() / db.pageSize) + 1, Page.PageType.FREE_LIST_PAGE);
            db.freeList.write(page);

            meta.setFreeList(page.getPageId());

            // Write dirty pages to disk.
            startTime = System.nanoTime();
            write();

            if (!db.noSync) {
                db.fileChannel.force(oldMaxPageId < meta.getMaxPageId());
            }

            // If strict mode is enabled then perform a consistency check.
            // Only the first consistency error is reported

            // If strict mode is enabled then perform a consistency check.
            // Only the first consistency error is reported in the panic.
            if (db.strictMode) {
                check();
            }

            writeMeta();
            stats.writeTime = System.nanoTime() - startTime;

            // Finalize the transaction.
            close();
        } catch (Exception e) {
            doRollback();
            throw new DbException("Error during commit", e);
        }

        for (var commitHandler : commitHandlers) {
            commitHandler.run();
        }
    }

    private void write() {
        ArrayList<Page> pages = new ArrayList<>(dirtyPages.values());
        pages.sort(PageIdComparator.INSTANCE);

        long initialPageId = 0;
        long prevPageId = 0;
        ArrayList<ByteBuffer> chunk = new ArrayList<>();

        //write pages by chunks
        for (var page : pages) {
            var pageId = page.getPageId();

            //start of the new chunk
            if (prevPageId == 0) {
                initialPageId = pageId;
                chunk.add(page.pageSegment.asByteBuffer());
            } else if (pageId == prevPageId + 1) {
                //adjacent page
                chunk.add(page.pageSegment.asByteBuffer());
            } else {
                //start of the new chunk, write chunk, reset statistics and add page to the new chunk
                writePageChunk(initialPageId, chunk);

                initialPageId = pageId;
                chunk.clear();
                chunk.add(page.pageSegment.asByteBuffer());
            }

            prevPageId = pageId;
        }

        //writing last chunk to the disk
        if (!chunk.isEmpty()) {
            writePageChunk(initialPageId, chunk);
        }
    }

    private void close() {
        if (db == null) {
            return;
        }

        // Put small pages back to page pool.
        if (dirtyPages != null) {
            for (var page : dirtyPages.values()) {
                if (page.getOverflow() > 0) {
                    continue;
                }

                var memorySegment = page.pageSegment;

                //noinspection resource
                assert memorySegment.scope() == db.poolScope;
                db.pagePool.push(memorySegment);
            }
            dirtyPages = null;
        }

        if (scope != null) {
            scope.close();
        }

        if (writable) {
            // Grab freelist stats.
            var freeListFreeN = db.freeList.freeCount();
            var freeListPendingN = db.freeList.pendingCount();
            var freeListAlloc = db.freeList.size();

            db.stats.freePageN = freeListFreeN;
            db.stats.pendingPageN = freeListPendingN;
            db.stats.freeAlloc = (freeListFreeN + freeListPendingN) * (long) db.pageSize;
            db.stats.freeListInUse = freeListAlloc;
            db.stats.txStats.add(stats);

            //update cache of db buckets
            for (var children : root.buckets.entrySet()) {
                var value = children.getValue().spilled;
                if (value != null) {
                    db.buckets.put(children.getKey(), value);
                }
            }

            db.rwTx = null;
            db.rwLock.unlock();
        } else {
            db.removeTx(this);
        }

        // Clear all references.
        db = null;
        meta = null;
        dirtyPages = null;
        root = null;
    }

    /**
     * Writes chunk of pages to the disk.
     *
     * @param startPageId Postion of first page in chunk.
     * @param chunk       Chunk of pages to write.
     */
    private void writePageChunk(long startPageId, ArrayList<ByteBuffer> chunk) {
        final long bytesToWrite = chunk.size() * (long) db.pageSize;
        long bytesWritten = 0;

        try {
            db.fileChannel.position(startPageId * db.pageSize);
            while (bytesWritten < bytesToWrite) {
                bytesWritten += db.fileChannel.write(chunk.toArray(new ByteBuffer[0]));
            }
        } catch (IOException e) {
            throw new DbException("Error during writing of dirty pages", e);
        }
    }

    /**
     * Writes the meta to the disk.
     */
    private void writeMeta() {
        if (meta.getRoot() >= meta.getMaxPageId()) {
            throw new DbException(
                    String.format("root bucket pgid (%d) above high water mark (%d)", meta.getRoot(),
                            meta.getMaxPageId()));
        }
        if (meta.getFreeList() >= meta.getMaxPageId()) {
            throw new DbException(
                    String.format("freelist pgid (%d) above high water mark (%d)",
                            meta.getFreeList(), meta.getMaxPageId()));
        }

        db.metaLock.exclusiveLock();
        try {
            var metaSegment = MemorySegment.ofArray(new byte[db.pageSize]);
            metaSegment.copyFrom(meta.pageSegment);

            // Page id is either going to be 0 or 1 which we can determine by the transaction ID.
            var meta = new Meta(metaSegment);
            meta.setPageId(meta.getTXId() % 2);
            meta.setFlags((short) (meta.getFlags() | Page.META_PAGE_FLAG));
            meta.generateCheckSum();

            try {
                db.fileChannel.write(meta.pageSegment.asByteBuffer(), meta.getPageId() * db.pageSize);
                if (!db.noSync) {
                    db.fileChannel.force(false);
                }
            } catch (IOException e) {
                throw new DbException("Error during writing of meta page");
            }
        } finally {
            db.metaLock.exclusiveUnlock();
        }

        stats.write++;
    }

    /**
     * Closes the transaction and ignores all previous updates.
     * Read-only transactions must be rolled back and not committed.
     */
    public void rollback() {
        assert !managed : "managed tx rollback not allowed";
        if (db == null) {
            throw new TransactionIsClosedException();
        }

        doRollback();
    }

    private void doRollback() {
        if (db == null) {
            return;
        }

        if (writable) {
            db.freeList.rollback(meta.getTXId());

            db.metaLock.sharedLock();
            try {
                db.freeList.reload(db.page(db.meta().getFreeList()));
            } finally {
                db.metaLock.sharedUnlock();
            }
        }

        close();
    }

    /**
     * Returns a reference to the page with a given id.
     * If page has been written to then a temporary buffered page is returned.
     *
     * @param pageId ID of the page to load.
     * @return returns a reference to the page with a given id.
     */
    Page page(long pageId) {
        // Check the dirty pages first.
        if (dirtyPages != null) {
            var page = dirtyPages.get(pageId);
            if (page != null) {
                return page;
            }
        }


        // Otherwise return directly from the mmap.
        return db.page(pageId);
    }


    /**
     * Allocates buffer which may consist of several DB pages.
     *
     * @param count    amount of DB pages inside of the block
     * @param pageType Type of the page
     * @return contiguous block of memory starting at a given page.
     */
    Page allocate(int count, Page.PageType pageType) {
        var page = db.allocate(count, this, pageType);

        dirtyPages.put(page.getPageId(), page);

        stats.pageCount++;
        stats.pageAlloc += count * (long) db.pageSize;

        return page;
    }

    public void check() {
        var executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(this::doCheck).get();
        } catch (InterruptedException e) {
            throw new DbException("Execution of check was interrupted", e);
        } catch (ExecutionException e) {
            throw new DbException("Check was executed with error", e);
        }
    }

    private void doCheck() {
        // Check if any pages are double freed.
        var freed = new LongOpenHashSet();
        var all = new LongArrayList();

        db.freeList.copyAll(all);

        for (int i = 0; i < all.size(); i++) {
            long pageId = all.getLong(i);

            if (!freed.add(pageId)) {
                throw new IllegalStateException(String.format("page %d: already freed", pageId));
            }
        }

        var reachable = new LongOpenHashSet();
        reachable.add(0);
        reachable.add(1);

        long freeListPageId = meta.getFreeList();
        var freeListPage = page(meta.getFreeList());

        for (int i = 0; i <= freeListPage.getOverflow(); i++) {
            reachable.add(i + freeListPageId);
        }

        checkBucket(root, reachable, freed);

        //Ensure all pages below high water mark are either reachable or freed.
        for (long pageId = 0; pageId < meta.getMaxPageId(); pageId++) {
            if (!reachable.contains(pageId) && !freed.contains(pageId)) {
                throw new IllegalStateException(String.format("page %d: unreachable unfreed", pageId));
            }
        }
    }

    private void checkBucket(Bucket bucket, LongOpenHashSet reachable, LongOpenHashSet freed) {
        // Ignore inline buckets.
        if (bucket.root == 0) {
            return;
        }

        forEachPage(bucket.root, 0, (page, depth) -> {
            if (page.getPageId() > meta.getMaxPageId()) {
                throw new IllegalStateException(String.format("page %d: out of bounds: %d",
                        page.getPageId(), meta.getMaxPageId()));
            }

            //Ensure each page is only referenced once.
            for (int i = 0; i <= page.getOverflow(); i++) {
                long pageId = page.getPageId() + i;
                var added = reachable.add(pageId);

                if (!added) {
                    throw new IllegalStateException(String.format("page %d: multiple references",
                            pageId));
                }

                // We should only encounter un-freed leaf and branch pages.
                if (freed.contains(pageId)) {
                    throw new IllegalStateException(String.format("page %d: reachable freed", pageId));
                }
            }
        });

        bucket.forEach((k, v) -> {
            if (v == null) {
                var b = Objects.requireNonNull(bucket.bucket(k));
                checkBucket(b, reachable, freed);
            }
        });
    }

    /**
     * Iterates over every page within a given page and executes a function.
     *
     * @param pageId   Starting page id.
     * @param depth    Starting depth of iteration.
     * @param consumer Consumer executed at each page. Accepts page and depth of iteration as parameters.
     */
    void forEachPage(long pageId, int depth, ObjIntConsumer<BTreePage> consumer) {
        var page = (BTreePage) page(pageId);

        // Execute function.
        consumer.accept(page, depth);

        // Recursively loop over children.
        if ((page.getFlags() & Page.BRANCH_PAGE_FLAG) != 0) {
            for (int i = 0; i < page.getCount(); i++) {
                var elem = page.getBranchElement(i);
                forEachPage(elem.getPageId(), depth + 1, consumer);
            }
        }
    }

    /**
     * Represents statistics about the actions performed by the transaction.
     */
    public final static class TxStats {
        //Page statistics
        /**
         * Number of page allocations
         */
        long pageCount;

        /**
         * Total bytes allocated
         */
        long pageAlloc;

        // Cursor statistics
        /**
         * Number of cursors created
         */
        long cursorCount;

        // Node statistics
        /**
         * Number of node allocations
         */
        long nodeCount;
        /**
         * number of node dereferences
         */
        long nodeDeref;

        // Rebalance statistics.
        /**
         * Number of node rebalances
         */
        long rebalance;

        /**
         * Total time spent rebalancing
         */
        long rebalanceTime;

        // Split/Spill statistics.
        /**
         * Number of nodes split
         */
        int split;

        /**
         * Number of nodes spilled
         */
        int spill;

        /**
         * Total time spent spilling
         */
        long spillTime;

        // Write statistics.
        /**
         * Number of writes performed
         */
        int write;

        /**
         * Total time spent writing to disk
         */
        long writeTime;

        void add(TxStats other) {
            pageCount += other.pageCount;
            pageAlloc += other.pageAlloc;
            cursorCount += other.cursorCount;
            nodeCount += other.nodeCount;
            nodeDeref += other.nodeDeref;
            rebalance += other.rebalance;
            rebalanceTime += other.rebalanceTime;
            split += other.split;
            spill += other.spill;
            spillTime += other.spillTime;
            write += other.write;
            writeTime += other.writeTime;
        }

        public TxStats sub(TxStats other) {
            var diff = new TxStats();

            diff.pageCount = pageCount - other.pageCount;
            diff.pageAlloc = pageAlloc - other.pageAlloc;
            diff.cursorCount = cursorCount - other.cursorCount;
            diff.nodeCount = nodeCount - other.nodeCount;
            diff.nodeDeref = nodeDeref - other.nodeDeref;
            diff.rebalance = rebalance - other.rebalance;
            diff.rebalanceTime -= other.rebalanceTime;
            diff.spill = spill - other.spill;
            diff.split = split - other.split;
            diff.spillTime -= other.spillTime;
            diff.write = write - diff.write;
            diff.writeTime -= diff.writeTime;

            return diff;
        }
    }

    private static final class PageIdComparator implements Comparator<Page> {
        static final PageIdComparator INSTANCE = new PageIdComparator();

        @Override
        public int compare(Page pageOne, Page pageTwo) {
            return Long.compare(pageOne.getPageId(), pageTwo.getPageId());
        }
    }
}
