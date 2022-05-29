package io.viktoriadb;

import io.viktoriadb.exceptions.DbException;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemoryLayouts;

import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

/**
 * FreeList represents a list of all pages that are available for allocation.
 * It also tracks pages that have been freed but are still in use by open transactions.
 */
final class FreeList {
    private static final MemoryLayout PAGE_LAYOUT = MemoryLayout.structLayout(Page.LAYOUT,
            MemoryLayout.paddingLayout(16),
            MemoryLayouts.JAVA_INT.withName("count"),
            MemoryLayout.paddingLayout(32),
            MemoryLayout.sequenceLayout(MemoryLayouts.JAVA_LONG).withName("pgids"));

    private static final long COUNT_HANDLE_OFFSET =
            PAGE_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("count"));
    private static final VarHandle COUNT_HANDLE = MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

    private static final long PAGE_IDS_HANDLE_BASE = PAGE_LAYOUT.byteOffset(
            MemoryLayout.PathElement.groupElement("pgids"), MemoryLayout.PathElement.sequenceElement(0));
    private static final VarHandle PAGE_IDS_HANDLE = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());
    /**
     * Creates new {@link LongArrayList} if it is absent into the <code>Map</code>.
     */
    private static final LongFunction<LongArrayList> createListForMap = (k) -> new LongArrayList();

    /**
     * All free and available free page ids.
     */
    LongArrayList ids = new LongArrayList();

    /**
     * Mapping of soon-to-be free page ids by tx.
     */
    final Long2ObjectOpenHashMap<LongArrayList> pending = new Long2ObjectOpenHashMap<>();

    /**
     * Fast lookup of all free and pending page ids.
     */
    final LongOpenHashSet cache = new LongOpenHashSet();

    /**
     * Removes page from cache by page id.
     */
    private final LongConsumer removePagesFromCache = cache::remove;

    /**
     * @return count of free pages on the freelist
     */
    int freeCount() {
        return ids.size();
    }

    /**
     * @return count of pending pages
     */
    int pendingCount() {
        int pendingCount = 0;
        for (var values : pending.values()) {
            pendingCount += values.size();
        }

        return pendingCount;
    }

    /**
     * @return count of pages on the freelist
     */
    int count() {
        return freeCount() + pendingCount();
    }

    /**
     * Returns the starting page id of a contiguous list of pages of a given size.
     * If a contiguous block cannot be found then 0 is returned.
     *
     * @param n Amount of pages to allocate
     * @return the starting page id of a contiguous list of pages of a given size.
     */
    long allocate(int n) {
        if (ids.isEmpty()) {
            return 0;
        }

        long prevId = 0;
        long initialId = 0;

        for (int i = 0; i < ids.size(); i++) {
            final long id = ids.getLong(i);
            if (id < 0) {
                throw new DbException(String.format("Invalid page allocation: %d", id));
            }

            // Reset initial page if this is not contiguous.
            if (prevId == 0 || id - prevId != 1) {
                initialId = id;
            }

            // If we found a contiguous block then remove it and return it.
            if (id - initialId + 1 == n) {
                ids.removeElements(i - n + 1, i + 1);

                // Remove from the free cache.

                for (long k = initialId; k < initialId + n; k++) {
                    var removed = cache.remove(k);

                    if (!removed) {
                        throw new IllegalStateException("Free list cache is broken");
                    }
                }

                return initialId;
            }
            prevId = id;
        }

        return 0;
    }

    /**
     * Copies all free and pending page ids into the destination array list.
     *
     * @param dst Destination list.
     */
    void copyAll(LongArrayList dst) {
        for (var pendingIds : pending.values()) {
            for (int i = 0; i < pendingIds.size(); i++) {
                dst.add(pendingIds.getLong(i));
            }
        }

        dst.addAll(ids);
    }

    /**
     * Releases a page and its overflow for a given transaction id.
     * If the page is already free then exception will be thrown.
     *
     * @param transactionId ID of transaction page of which is going to be freed.
     * @param page          Page to release.
     */
    void free(long transactionId, Page page) {
        if (page.getPageId() <= 1) {
            throw new DbException(String.format("Can not free page 0 or 1 : %d", page.getPageId()));
        }

        var ids = pending.computeIfAbsent(transactionId, createListForMap);
        for (long id = page.getPageId(); id <= page.getPageId() + page.getOverflow(); id++) {
            if (!cache.add(id)) {
                throw new DbException(String.format("Page with id %d is already freed", id));
            }

            ids.add(id);
        }
    }

    /**
     * Moves all page ids for a transaction id (or older) to the freelist.
     *
     * @param transactionsId ID of transactions pages of which should be moved to the free list
     */
    void release(long transactionsId) {
        var iterator = pending.long2ObjectEntrySet().fastIterator();

        while (iterator.hasNext()) {
            var entry = iterator.next();
            var txId = entry.getLongKey();

            if (txId <= transactionsId) {
                ids.addAll(entry.getValue());
                iterator.remove();
            }
        }

        ids.sort(null);
    }

    /**
     * Removes the pages from a given pending tx.
     *
     * @param transactionId ID of transaction
     */
    void rollback(long transactionId) {
        var ids = pending.remove(transactionId);
        if (ids != null) {
            ids.forEach(removePagesFromCache);
        }
    }

    /**
     * @param pageId Page ID.
     * @return Whether a given page is in the free list.
     */
    boolean freed(long pageId) {
        return cache.contains(pageId);
    }


    /**
     * writes the page ids onto a freelist page. All free and pending ids are
     * saved to disk since in the event of a program crash, all pending ids will
     * become free.
     *
     * @param page FreeList page.
     */
    void write(Page page) {
        var count = count();
        page.setFlags((short) (page.getFlags() | Page.LEAF_PAGE_FLAG));

        var pageSegment = page.pageSegment;

        COUNT_HANDLE.set(pageSegment, COUNT_HANDLE_OFFSET, count);

        int index = 0;
        for (int i = 0; i < ids.size(); i++) {
            var pageId = ids.getLong(i);
            PAGE_IDS_HANDLE.set(pageSegment, ((long) index) * Long.BYTES + PAGE_IDS_HANDLE_BASE, pageId);
            index++;
        }

        for (var ids : pending.values()) {
            for (int i = 0; i < ids.size(); i++) {
                var pageId = ids.getLong(i);
                PAGE_IDS_HANDLE.set(pageSegment, ((long) index) * Long.BYTES + PAGE_IDS_HANDLE_BASE, pageId);
                index++;
            }
        }
    }

    /**
     * Initializes the freelist from a freelist page.
     *
     * @param page FreeList page
     */
    void read(Page page) {
        var pageSegment = page.pageSegment;

        int count = (int) COUNT_HANDLE.get(pageSegment, COUNT_HANDLE_OFFSET);
        ids.clear();

        for (int i = 0; i < count; i++) {
            final long pageId = (long) PAGE_IDS_HANDLE.get(pageSegment, PAGE_IDS_HANDLE_BASE + ((long) i) * Long.BYTES);
            ids.add(pageId);
        }

        ids.sort(null);

        reindex();
    }

    /**
     * Reads the freelist from a page and filters out pending items.
     *
     * @param page On disk presentation of free page.
     */
    void reload(Page page) {
        read(page);

        // Build a cache of only pending pages.
        LongOpenHashSet pcache = new LongOpenHashSet();
        for (var pendingIds : pending.values()) {
            for (int i = 0; i < pendingIds.size(); i++) {
                var pid = pendingIds.getLong(i);
                pcache.add(pid);
            }
        }

        // Check each page in the freelist and build a new available freelist
        // with any pages not in the pending lists.

        LongArrayList a = new LongArrayList();
        for (int i = 0; i < ids.size(); i++) {
            var pid = ids.getLong(i);
            if (!pcache.contains(pid)) {
                a.add(pid);
            }
        }

        ids = a;

        // Once the available list is rebuilt then rebuild the free cache so that
        // it includes the available and pending free pages.
        reindex();
    }

    /**
     * Rebuilds the free cache based on available and pending free lists.
     */
    private void reindex() {
        cache.clear();

        for (int i = 0; i < ids.size(); i++) {
            cache.add(ids.getLong(i));
        }

        for (var pendingIds : pending.values()) {
            for (int i = 0; i < pendingIds.size(); i++) {
                cache.add(pendingIds.getLong(i));
            }
        }
    }

    /**
     * @return the size of the page after serialization.
     */
    int size() {
        long headerSize = PAGE_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("pgids"));
        return (int) (headerSize + count() * Long.BYTES);
    }
}
