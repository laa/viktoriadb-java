package io.viktoriadb;

import io.viktoriadb.exceptions.DbException;
import io.viktoriadb.exceptions.InvalidCheckSumException;
import io.viktoriadb.exceptions.InvalidMagicException;
import io.viktoriadb.exceptions.InvalidVersionFormatException;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemoryLayouts;
import jdk.incubator.foreign.MemorySegment;
import net.jpountz.xxhash.XXHashFactory;

import java.lang.invoke.VarHandle;

final class Meta extends Page {
    private static final long XX_HASH_SEED = 0x420ADEF;
    private static final XXHashFactory XX_HASH_FACTORY = XXHashFactory.fastestInstance();

    static final MemoryLayout LAYOUT = MemoryLayout.structLayout(
            Page.LAYOUT,
            MemoryLayouts.PAD_16,
            MemoryLayouts.JAVA_LONG.withName("root"),
            MemoryLayouts.JAVA_LONG.withName("freelist"),
            MemoryLayouts.JAVA_LONG.withName("pgid"),
            MemoryLayouts.JAVA_LONG.withName("txid"),
            MemoryLayouts.JAVA_LONG.withName("hash"),
            MemoryLayouts.JAVA_INT.withName("magic"),
            MemoryLayouts.JAVA_INT.withName("version"),
            MemoryLayouts.JAVA_INT.withName("pageSize")
    );
    private static final VarHandle ROOT_HANDLE = LAYOUT.varHandle(long.class,
            MemoryLayout.PathElement.groupElement("root"));
    private static final VarHandle FREE_LIST_HANDLE = LAYOUT.varHandle(long.class,
            MemoryLayout.PathElement.groupElement("freelist"));
    private static final VarHandle PGID_HANDLE = LAYOUT.varHandle(long.class,
            MemoryLayout.PathElement.groupElement("pgid"));
    private static final VarHandle TXID_HANDLE = LAYOUT.varHandle(long.class,
            MemoryLayout.PathElement.groupElement("txid"));
    private static final VarHandle MAGIC_HANDLE = LAYOUT.varHandle(int.class,
            MemoryLayout.PathElement.groupElement("magic"));
    private static final VarHandle VERSION_HANDLE = LAYOUT.varHandle(int.class,
            MemoryLayout.PathElement.groupElement("version"));
    private static final VarHandle PAGE_SIZE_HANDLE = LAYOUT.varHandle(int.class,
            MemoryLayout.PathElement.groupElement("pageSize"));
    private static final VarHandle HASH_HANDLE = LAYOUT.varHandle(long.class,
            MemoryLayout.PathElement.groupElement("hash"));

    Meta(MemorySegment pageSegment) {
        super(pageSegment);
    }

    long getRoot() {
        return (long) ROOT_HANDLE.get(pageSegment);
    }

    void setRoot(long root) {
        ROOT_HANDLE.set(pageSegment, root);
    }

    long getFreeList() {
        return (long) FREE_LIST_HANDLE.get(pageSegment);
    }

    void setFreeList(long freeList) {
        FREE_LIST_HANDLE.set(pageSegment, freeList);
    }

    long getMaxPageId() {
        return (long) PGID_HANDLE.get(pageSegment);
    }

    void setMaxPageId(long maxPageId) {
        PGID_HANDLE.set(pageSegment, maxPageId);
    }

    long getTXId() {
        return (long) TXID_HANDLE.get(pageSegment);
    }

    void setTXId(long txid) {
        TXID_HANDLE.set(pageSegment, txid);
    }

    int getMagic() {
        return (int) MAGIC_HANDLE.get(pageSegment);
    }

    void setMagic() {
        MAGIC_HANDLE.set(pageSegment, DB.MAGIC);
    }

    int getVersion() {
        return (int) VERSION_HANDLE.get(pageSegment);
    }

    void setVersion() {
        VERSION_HANDLE.set(pageSegment, DB.VERSION);
    }

    int getPageSize() {
        return (int) PAGE_SIZE_HANDLE.get(pageSegment);
    }

    void setPageSize(int pageSize) {
        PAGE_SIZE_HANDLE.set(pageSegment, pageSize);
    }

    void generateCheckSum() {
        final long offset = LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("hash"));
        var xxHash = XX_HASH_FACTORY.hash64();
        var hash = xxHash.hash(pageSegment.asSlice(0, offset).asByteBuffer(), XX_HASH_SEED);
        HASH_HANDLE.set(pageSegment, hash);
    }

    /**
     * Checks the marker bytes and version of the meta page to ensure it matches this binary.
     *
     * @throws DbException if check is failed.
     */
    void validate() {
        final int magic = getMagic();
        if (magic != DB.MAGIC) {
            throw new InvalidMagicException();
        }

        final int version = getVersion();
        if (version != DB.VERSION) {
            throw new InvalidVersionFormatException();
        }

        final long storedHash = (long) HASH_HANDLE.get(pageSegment);
        var xxHash = XX_HASH_FACTORY.hash64();
        final long offset = LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("hash"));
        var hash = xxHash.hash(pageSegment.asSlice(0, offset).asByteBuffer(), XX_HASH_SEED);

        if (hash != storedHash) {
            throw new InvalidCheckSumException();
        }
    }
}
