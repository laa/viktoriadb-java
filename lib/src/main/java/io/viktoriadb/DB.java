package io.viktoriadb;

import io.viktoriadb.exceptions.DatabaseIsReadOnlyException;
import io.viktoriadb.exceptions.DbException;
import io.viktoriadb.exceptions.DbIsNotOpenedException;
import io.viktoriadb.util.ScalableStampedReentrantRWLock;
import it.unimi.dsi.fastutil.Function;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public final class DB implements Closeable {
    /**
     * The data file format version.
     */
    static final int VERSION = 2;

    /**
     * The data file format version.
     */
    static final int MAGIC = 0xED0CDAED;

    /**
     * Default page size for DB
     */
    private static final int DEFAULT_PAGE_SIZE = 4 * 1024;

    /**
     * DefaultOptions represent the options used if nil options are passed into {@link DB#open(Path, Options)}.
     * No timeout is used which will cause Bolt to wait indefinitely for a lock.
     */
    private static final Options DEFAULT_OPTIONS = new Options(false, false);


    /**
     * When enabled, the database will perform a check() after every commit.
     * A panic is issued if the database is in an inconsistent state. This
     * flag has a large performance impact so it should only be used for
     * debugging purposes.
     */
    boolean strictMode;

    /**
     * Setting the noSync flag will cause the database to skip fsync()
     * calls after each commit. This can be useful when bulk loading data
     * into a database and you can restart the bulk load in the event of
     * a system failure or database corruption. Do not set this flag for
     * normal use.
     * <p>
     * If the package global IgnoreNoSync constant is true, this value is
     * ignored.  See the comment on that constant for more details.
     * <p>
     * THIS IS UNSAFE. PLEASE USE WITH CAUTION.
     */
    final boolean noSync;

    private final Path path;

    FileChannel fileChannel;
    RandomAccessFile file;
    private final FileLock fileLock;

    ResourceScope poolScope;
    private ResourceScope mmapScope;

    private long dataSz;

    private Meta meta0;
    private Meta meta1;

    private final boolean readOnly;

    final int pageSize;
    private boolean opened;
    Tx rwTx;

    private MemorySegment mmapSegment;

    private final ConcurrentSkipListSet<Tx> txs = new ConcurrentSkipListSet<>(Comparator.comparingLong(Tx::id));
    final FreeList freeList = new FreeList();

    Stats stats = new Stats();

    /**
     * Pool of pages allocated using native memory.
     * It is not thread safe because all allocations/deallocations
     * are performed inside write transactions which protected by {@link  #rwLock}.
     */
    final ObjectArrayList<MemorySegment> pagePool = new ObjectArrayList<>();

    /**
     * Allows only one writer at a time.
     */
    final ReentrantLock rwLock = new ReentrantLock();

    /**
     * Protects meta page access.
     */
    final ScalableStampedReentrantRWLock metaLock = new ScalableStampedReentrantRWLock();

    /**
     * Protects mmap access during remapping.
     */
    private final ScalableStampedReentrantRWLock mMapLock = new ScalableStampedReentrantRWLock();

    public static DB open(Path path, Options options) {
        // Set default options if no options are provided.
        if (options == null) {
            options = DEFAULT_OPTIONS;
        }

        final RandomAccessFile file;
        if (!options.readOnly()) {
            try {
                file = new RandomAccessFile(path.toFile(), "rw");
            } catch (IOException e) {
                throw new DbException("Error during opening of file", e);
            }
        } else {
            file = null;
        }

        final FileChannel fileChannel;
        try {
            if (options.readOnly()) {
                fileChannel = FileChannel.open(path, StandardOpenOption.READ);
            } else {
                fileChannel = file.getChannel();
            }
        } catch (final IOException e) {
            throw new DbException("Error during opening of file channel", e);
        }


        final FileLock fileLock;
        if (!options.readOnly()) {
            try {
                fileLock = fileChannel.lock();
            } catch (IOException e) {
                try {
                    fileChannel.close();
                } catch (IOException ex) {
                    //ignore
                }

                throw new DbException("Error during locking of file", e);
            }
        } else {
            fileLock = null;
        }

        var storage = new DB(options.noSync(), path, file, fileChannel, fileLock,
                options.readOnly(), DEFAULT_PAGE_SIZE);

        try {
            storage.opened = true;
            storage.poolScope = ResourceScope.newSharedScope();

            if (fileChannel.size() == 0) {
                // Initialize the database if it doesn't exist.
                storage.init();
            }

            storage.mmap(2L * storage.pageSize);
            storage.freeList.read(storage.page(storage.meta().getFreeList()));
        } catch (Exception e) {
            storage.doClose();

            if (e instanceof DbException) {
                throw (DbException) e;
            } else {
                throw new DbException("Error during opening of the database", e);
            }
        }

        return storage;
    }

    private DB(boolean noSync, Path path, RandomAccessFile file, FileChannel fileChannel, FileLock fileLock,
               boolean readOnly, int pageSize) {
        this.noSync = noSync;
        this.path = path;
        this.fileChannel = fileChannel;
        this.file = file;
        this.fileLock = fileLock;
        this.readOnly = readOnly;
        this.pageSize = pageSize;
    }

    private void init() {
        final MemorySegment data = MemorySegment.ofArray(new byte[pageSize * 4]);

        for (int i = 0; i < 2; i++) {
            Page page = new Page(data.asSlice(i * pageSize, pageSize));
            page.setPageId(i);
            page.setFlags(Page.META_PAGE_FLAG);

            var meta = (Meta) Page.createExistingPage(page.pageSegment);
            meta.setMagic();
            meta.setVersion();
            meta.setPageSize(pageSize);
            meta.setFreeList(2);
            meta.setRoot(3);
            meta.setPageId(i);
            meta.setMaxPageId(4);
            meta.setTXId(i);
            meta.generateCheckSum();
        }

        final Page freeListPage = new Page(data.asSlice(2L * pageSize, pageSize));
        freeListPage.setPageId(2);
        freeListPage.setFlags(Page.FREE_LIST_PAGE_FLAG);

        final Page rootPage = new Page(data.asSlice(3L * pageSize, pageSize));
        rootPage.setPageId(3);
        rootPage.setFlags(Page.LEAF_PAGE_FLAG);

        final ByteBuffer buffer = data.asByteBuffer();
        try {
            while (buffer.remaining() > 0) {
                fileChannel.write(buffer);
            }
            fileChannel.force(true);
        } catch (IOException e) {
            throw new DbException("Error during database initialization", e);
        }
    }

    /**
     * Opens the underlying memory-mapped file and initializes the meta references.
     *
     * @param minsz is the minimum size that the new mmap can be.
     */
    private void mmap(long minsz) {
        mMapLock.exclusiveLock();
        try {
            long fileSize = fileChannel.size();
            final long initialSize = fileSize;

            if (fileSize < 2L * pageSize) {
                throw new DbException(String.format("File size is too small - %d", fileSize));
            }

            if (fileSize < minsz) {
                fileSize = minsz;
            }

            fileSize = mmapSize(fileSize);

            // Dereference all mmap references before unmapping.
            if (rwTx != null) {
                rwTx.root.dereference();
            }

            // Unmap existing data before continuing.
            if (mmapScope != null) {
                mmapScope.close();
            }

            if (initialSize < fileSize) {
                file.setLength(fileSize);
            }

            mmapScope = ResourceScope.newSharedScope();
            mmapSegment = MemorySegment.mapFile(path, 0, fileSize,
                    FileChannel.MapMode.READ_ONLY, mmapScope);

            meta0 = (Meta) page(0);
            meta1 = (Meta) page(1);

            // Validate the meta pages. We only return an error if both meta pages fail
            // validation, since meta0 failing validation means that it wasn't saved
            // properly -- but we can recover using meta1. And vice-versa.
            boolean err0 = false;
            boolean err1 = false;

            try {
                meta0.validate();
            } catch (Exception e) {
                meta0.setTXId(-1);
                err0 = true;
            }

            try {
                meta1.validate();
            } catch (Exception e) {
                meta1.setTXId(-1);
                err1 = true;
            }

            if (err0 && err1) {
                throw new DbException("Validation was failed");
            }

            dataSz = fileSize;

        } catch (IOException e) {
            throw new DbException("Error during file mapping", e);
        } finally {
            mMapLock.exclusiveUnlock();
        }
    }


    /**
     * Determines the appropriate size for the mmap given the current size
     * of the database. The minimum size is 32KB and doubles until it reaches 1GB.
     *
     * @param size Current MMAP size.
     * @return New MMAP size.
     */
    private long mmapSize(long size) {
        // Double the size from 32KB until 1GB.
        for (long i = 15; i <= 30; i++) {
            if (size <= (1 << i)) {
                return 1 << i;
            }
        }

        // If larger than 1GB then grow by 1GB at a time.
        long sz = size;
        long reminder = sz % (1 << 30);
        if (reminder > 0) {
            sz += (1 << 30) - reminder;
        }

        // Ensure that the mmap size is a multiple of the page size.
        // This should always be true since we're incrementing in MBs.
        if (sz % pageSize != 0) {
            sz = ((sz / pageSize) + 1) * pageSize;
        }

        return sz;
    }

    /**
     * Releases all database resources.
     * All transactions must be closed before closing the database.
     */
    public void close() {
        rwLock.lock();
        try {
            metaLock.exclusiveLock();
            try {
                mMapLock.sharedLock();
                try {
                    doClose();
                } finally {
                    mMapLock.sharedUnlock();
                }
            } finally {
                metaLock.exclusiveLock();
            }
        } finally {
            rwLock.unlock();
        }
    }

    private void doClose() {
        if (!opened) {
            return;
        }

        opened = false;
        if (mmapScope != null) {
            mmapScope.close();
        }

        if (poolScope != null) {
            poolScope.close();
        }


        try {
            if (fileChannel != null) {
                fileChannel.force(true);
            }

            if (fileLock != null) {
                fileLock.release();
                fileLock.close();
            }

            if (file != null) {
                file.close();
            }
            if (fileChannel != null) {
                fileChannel.close();
            }
        } catch (IOException e) {
            throw new DbException("Error during closing of database", e);
        }
    }

    /**
     * Starts a new transaction.
     * Multiple read-only transactions can be used concurrently but only one
     * write transaction can be used at a time. Starting multiple write transactions
     * will cause the calls to block and be serialized until the current write
     * transaction finishes.
     * <p>
     * Transactions should not be dependent on one another. Opening a read
     * transaction and a write transaction in the same goroutine can cause the
     * writer to deadlock because the database periodically needs to re-mmap itself
     * as it grows and it cannot do that while a read transaction is open.
     * <p>
     * IMPORTANT: You must close read-only transactions after you are finished or
     * else the database will not reclaim old pages.
     *
     * @param writable Indicates if transaction is writable
     * @return New transaction instance.
     */
    public Tx begin(boolean writable) {
        if (writable) {
            return beginRWTx();
        }

        return beginTx();
    }

    private Tx beginTx() {
        Tx tx;
        // Lock the meta pages while we initialize the transaction. We obtain
        // the meta lock before the mmap lock because that's the order that the
        // write transaction will obtain them.
        metaLock.sharedLock();
        try {
            // Obtain a read-only lock on the mmap. When the mmap is remapped it will
            // obtain a write lock so all transactions must finish before it can be
            // remapped.
            mMapLock.sharedLock();
            if (!opened) {
                mMapLock.sharedUnlock();
                throw new DbIsNotOpenedException();
            }

            tx = new Tx(this, false);
            txs.add(tx);
        } finally {
            metaLock.sharedUnlock();
        }

        stats.openTxN.increment();
        stats.txN.increment();

        return tx;
    }

    private Tx beginRWTx() {
        if (readOnly) {
            throw new DatabaseIsReadOnlyException();
        }

        // Obtain writer lock. This is released by the transaction when it closes.
        // This enforces only one writer transaction at a time.
        rwLock.lock();

        // Once we have the writer lock then we can lock the meta pages so that
        // we can set up the transaction.
        metaLock.sharedLock();
        try {
            if (!opened) {
                throw new DbIsNotOpenedException();
            }

            rwTx = new Tx(this, true);

        } finally {
            metaLock.sharedUnlock();
        }

        long mindid = Long.MAX_VALUE;
        // Free any pages associated with closed read-only transactions.
        if (!txs.isEmpty()) {
            try {
                var firstTx = txs.first();
                mindid = firstTx.id();
            } catch (NoSuchElementException e) {
                //ignore
            }
        }

        freeList.release(mindid - 1);

        return rwTx;
    }

    Page page(long pageId) {
        final long pos = pageId * pageSize;
        return Page.createExistingPage(mmapSegment.asSlice(pos));
    }

    /**
     * A contiguous block of memory starting at a given page.
     *
     * @param count Size of block in pages
     * @param tx    Currently running transaction
     * @return Block of memory.
     */
    Page allocate(int count, Tx tx, Page.PageType pageType) {
        if (!tx.writable) {
            throw new DbException("Only writable transactions can allocate pages.");
        }

        MemorySegment buf = null;

        //all new buffers with size more than one page are living only till the life
        //of the transaction because they are allocated rarely
        //but buffers of one size are reused till the end of life of database.
        //such pages are used only to write new pages during write transaction
        //read transactions or already existing pages in new transaction
        //use pages backed by mmap.

        if (count == 1) {
            if (!pagePool.isEmpty()) {
                buf = pagePool.pop();

                //noinspection resource
                assert buf.scope() == poolScope;
            }

            if (buf == null) {
                buf = MemorySegment.allocateNative(pageSize, poolScope);
            }
        } else {
            buf = MemorySegment.allocateNative((long) count * pageSize, tx.scope);
        }

        var page = Page.createNewPage(buf, pageType);
        page.setOverflow(count - 1);

        var pageId = freeList.allocate(count);

        //reuse page which already does not contain data
        if (pageId > 0) {
            page.setPageId(pageId);
            return page;
        }

        pageId = rwTx.meta.getMaxPageId();
        page.setPageId(pageId);
        var minsz = (pageId + count + 1) * pageSize;

        if (minsz > dataSz) {
            mmap(minsz);
        }

        rwTx.meta.setMaxPageId(rwTx.meta.getMaxPageId() + count);

        return page;
    }


    /**
     * @return retrieves the current meta page reference.
     */
    Meta meta() {
        if (meta1.getTXId() > meta0.getTXId()) {
            if (meta1.getTXId() < 0) {
                throw new IllegalStateException("DB metadata is broken and can not be used");
            }
            return meta1;
        }

        if (meta0.getTXId() < 0) {
            throw new IllegalStateException("DB metadata is broken and can not be used");
        }

        return meta0;
    }

    /**
     * Removes a read transaction from the database.
     *
     * @param tx transaction to remove.
     */
    void removeTx(Tx tx) {
        mMapLock.sharedUnlock();

        var removed = txs.remove(tx);
        assert removed;

        stats.openTxN.decrement();
        stats.txStats.add(tx.stats);
    }

    /**
     * @return Path returns the path to currently open database file.
     */
    public Path path() {
        return path;
    }

    @Override
    public String toString() {
        return String.format("ViktoriaDB{path:%s}", path.toString());
    }

    /**
     * Executes a function within the context of a read-write managed transaction.
     * If no exception is thrown from the function then the transaction is committed.
     * If an exception is thrown then the entire transaction is rolled back.
     * <p>
     * Attempting to manually commit or rollback within the function will cause an exception.
     *
     * @return Result of execution.
     */
    public <V> V calculateInsideWriteTx(Function<Tx, V> code) {
        var tx = begin(true);
        tx.managed = true;

        V result;
        try {
            result = code.apply(tx);
        } catch (Exception e) {
            tx.managed = false;
            tx.rollback();

            if (e instanceof DbException) {
                throw (DbException) e;
            } else {
                throw new DbException("Error during execution of read-write transaction", e);
            }
        }

        tx.managed = false;
        tx.commit();

        return result;
    }

    /**
     * Executes a function within the context of a read-write managed transaction.
     * If no exception is thrown from the function then the transaction is committed.
     * If an exception is thrown then the entire transaction is rolled back.
     * <p>
     * Attempting to manually commit or rollback within the function will cause an exception.
     */
    public void executeInsideWriteTx(Consumer<Tx> code) {
        var tx = begin(true);
        tx.managed = true;

        try {
            code.accept(tx);
        } catch (Exception e) {
            tx.managed = false;
            tx.rollback();

            if (e instanceof DbException) {
                throw (DbException) e;
            } else {
                throw new DbException("Error during execution of read-write transaction", e);
            }
        }

        tx.managed = false;
        tx.commit();
    }

    /**
     * Executes a function within the context of a managed read-only transaction.
     * <p>
     * Attempting to manually rollback within the function will cause an exception.
     *
     * @param code Code to execute.
     * @param <V>  Return type of result.
     * @return Result of execution of code.
     */
    public <V> V calculateInReadTx(Function<Tx, V> code) {
        var tx = begin(false);
        tx.managed = true;

        V result;
        try {
            result = code.apply(tx);
        } catch (Exception e) {
            tx.managed = false;
            tx.rollback();

            if (e instanceof DbException) {
                throw (DbException) e;
            } else {
                throw new DbException("Error during execution of read-only transaction", e);
            }
        }

        tx.managed = false;
        tx.rollback();
        return result;
    }

    /**
     * Executes a function within the context of a managed read-only transaction.
     * <p>
     * Attempting to manually rollback within the function will cause an exception.
     *
     * @param code Code to execute.
     */
    public void executeInReadTx(Consumer<Tx> code) {
        var tx = begin(false);
        tx.managed = true;

        try {
            code.accept(tx);
        } catch (Exception e) {
            tx.managed = false;
            tx.rollback();

            if (e instanceof DbException) {
                throw (DbException) e;
            } else {
                throw new DbException("Error during execution of read-only transaction", e);
            }
        }

        tx.managed = false;
        tx.rollback();
    }
}
