package io.viktoriadb;

import io.viktoriadb.exceptions.DbException;
import io.viktoriadb.exceptions.InvalidCheckSumException;
import io.viktoriadb.exceptions.InvalidMagicException;
import io.viktoriadb.exceptions.InvalidVersionFormatException;
import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemoryLayouts;
import jdk.incubator.foreign.MemorySegment;
import net.jpountz.xxhash.XXHashFactory;

import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

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

    private static final long ROOT_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("root"));
    private static final VarHandle ROOT_HANDLE = MemoryHandles.varHandle(long.class,
            ByteOrder.nativeOrder());

    private static final long FREE_LIST_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("freelist"));
    private static final VarHandle FREE_LIST_HANDLE =
            MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());

    private static final long PGID_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("pgid"));
    private static final VarHandle PGID_HANDLE =
            MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());

    private static final long TXID_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("txid"));
    private static final VarHandle TXID_HANDLE = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());

    private static final long MAGIC_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("magic"));
    private static final VarHandle MAGIC_HANDLE = MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

    private static final long VERSION_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("version"));
    private static final VarHandle VERSION_HANDLE = MemoryHandles.varHandle(int.class,
            ByteOrder.nativeOrder());

    private static final long PAGE_SIZE_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("pageSize"));
    private static final VarHandle PAGE_SIZE_HANDLE = MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

    private static final long HASH_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("hash"));
    private static final VarHandle HASH_HANDLE = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());

    Meta(MemorySegment pageSegment) {
        super(pageSegment);
    }

    @Override
    void init() {
        super.init();

        setRoot(0);
        setFreeList(0);
        setMaxPageId(0);
        setTXId(0);
        setMagic();
        setVersion();
        setPageSize(0);
    }

    long getRoot() {
        return (long) ROOT_HANDLE.get(pageSegment, ROOT_HANDLE_OFFSET);
    }

    void setRoot(long root) {
        ROOT_HANDLE.set(pageSegment, ROOT_HANDLE_OFFSET, root);
    }

    long getFreeList() {
        return (long) FREE_LIST_HANDLE.get(pageSegment, FREE_LIST_HANDLE_OFFSET);
    }

    void setFreeList(long freeList) {
        FREE_LIST_HANDLE.set(pageSegment, FREE_LIST_HANDLE_OFFSET, freeList);
    }

    long getMaxPageId() {
        return (long) PGID_HANDLE.get(pageSegment, PGID_HANDLE_OFFSET);
    }

    void setMaxPageId(long maxPageId) {
        PGID_HANDLE.set(pageSegment, PGID_HANDLE_OFFSET, maxPageId);
    }

    long getTXId() {
        return (long) TXID_HANDLE.get(pageSegment, TXID_HANDLE_OFFSET);
    }

    void setTXId(long txid) {
        TXID_HANDLE.set(pageSegment, TXID_HANDLE_OFFSET, txid);
    }

    int getMagic() {
        return (int) MAGIC_HANDLE.get(pageSegment, MAGIC_HANDLE_OFFSET);
    }

    void setMagic() {
        MAGIC_HANDLE.set(pageSegment, MAGIC_HANDLE_OFFSET, DB.MAGIC);
    }

    int getVersion() {
        return (int) VERSION_HANDLE.get(pageSegment, VERSION_HANDLE_OFFSET);
    }

    void setVersion() {
        VERSION_HANDLE.set(pageSegment, VERSION_HANDLE_OFFSET, DB.VERSION);
    }

    int getPageSize() {
        return (int) PAGE_SIZE_HANDLE.get(pageSegment, PAGE_SIZE_HANDLE_OFFSET);
    }

    void setPageSize(int pageSize) {
        PAGE_SIZE_HANDLE.set(pageSegment, PAGE_SIZE_HANDLE_OFFSET, pageSize);
    }

    void generateCheckSum() {
        var xxHash = XX_HASH_FACTORY.hash64();
        var hash = xxHash.hash(pageSegment.asSlice(0, HASH_HANDLE_OFFSET).asByteBuffer(), XX_HASH_SEED);
        HASH_HANDLE.set(pageSegment, HASH_HANDLE_OFFSET, hash);
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

        final long storedHash = (long) HASH_HANDLE.get(pageSegment, HASH_HANDLE_OFFSET);
        var xxHash = XX_HASH_FACTORY.hash64();
        final long offset = LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("hash"));
        var hash = xxHash.hash(pageSegment.asSlice(0, offset).asByteBuffer(), XX_HASH_SEED);

        if (hash != storedHash) {
            throw new InvalidCheckSumException();
        }
    }
}
