package io.viktoriadb;

import io.viktoriadb.exceptions.DbException;
import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemoryLayouts;
import jdk.incubator.foreign.MemorySegment;

import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

class Page {
    static final MemoryLayout LAYOUT = MemoryLayout.structLayout(
            MemoryLayouts.JAVA_LONG.withName("id"),
            MemoryLayouts.JAVA_INT.withName("overflow"),
            MemoryLayouts.JAVA_SHORT.withName("flags")
    );

    static final short BRANCH_PAGE_FLAG = 0x01;
    static final short LEAF_PAGE_FLAG = 0x02;
    static final short META_PAGE_FLAG = 0x04;
    static final short FREE_LIST_PAGE_FLAG = 0x08;

    private static final long ID_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("id"));
    private static final VarHandle ID_HANDLE = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());

    private static final long OVERFLOW_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("overflow"));
    private static final VarHandle OVERFLOW_HANDLE = MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

    private static final long FLAGS_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("flags"));
    private static final VarHandle FLAGS_HANDLE = MemoryHandles.varHandle(short.class, ByteOrder.nativeOrder());

    final MemorySegment pageSegment;

    /**
     * Creates page object from memory segment which presents existing page.
     *
     * @param pageSegment Memory segment which represents existing object.
     * @return Page instance
     */
    static Page createExistingPage(MemorySegment pageSegment) {
        var flags = (short) FLAGS_HANDLE.get(pageSegment, FLAGS_HANDLE_OFFSET);

        if (((flags & BRANCH_PAGE_FLAG) != 0) || ((flags & LEAF_PAGE_FLAG) != 0)) {
            return new BTreePage(pageSegment);
        }
        if ((flags & META_PAGE_FLAG) != 0) {
            return new Meta(pageSegment);
        }
        if ((flags & FREE_LIST_PAGE_FLAG) != 0) {
            return new Page(pageSegment);
        }

        throw new DbException(String.format("Invalid page type flags = (%x)", flags));
    }


    /**
     * Creates page object from memory segment which presents new not initialized page.
     *
     * @param pageSegment Memory segment which represents new not initialized object.
     * @return Page instance.
     */
    static Page createNewPage(MemorySegment pageSegment, PageType pageType) {
        var flag = switch (pageType) {
            case LEAF_PAGE -> LEAF_PAGE_FLAG;
            case BRANCH_PAGE -> BRANCH_PAGE_FLAG;
            case META_PAGE -> META_PAGE_FLAG;
            case FREE_LIST_PAGE -> FREE_LIST_PAGE_FLAG;
        };

        FLAGS_HANDLE.set(pageSegment, FLAGS_HANDLE_OFFSET, flag);
        var page = createExistingPage(pageSegment);
        page.init();

        return page;
    }

    Page(MemorySegment pageSegment) {
        this.pageSegment = pageSegment;
    }

    void init() {
        setPageId(0);
        setOverflow(0);
    }

    final void setPageId(long pageId) {
        ID_HANDLE.set(pageSegment, ID_HANDLE_OFFSET, pageId);
    }

    final long getPageId() {
        return (long) ID_HANDLE.get(pageSegment, ID_HANDLE_OFFSET);
    }

    final void setOverflow(int overflow) {
        OVERFLOW_HANDLE.set(pageSegment, OVERFLOW_HANDLE_OFFSET, overflow);
    }

    final int getOverflow() {
        return (int) OVERFLOW_HANDLE.get(pageSegment, OVERFLOW_HANDLE_OFFSET);
    }

    final void setFlags(short flags) {
        FLAGS_HANDLE.set(pageSegment, FLAGS_HANDLE_OFFSET, flags);
    }

    final short getFlags() {
        return (short) FLAGS_HANDLE.get(pageSegment, FLAGS_HANDLE_OFFSET);
    }

    enum PageType {
        LEAF_PAGE,
        BRANCH_PAGE,
        META_PAGE,
        FREE_LIST_PAGE
    }
}
