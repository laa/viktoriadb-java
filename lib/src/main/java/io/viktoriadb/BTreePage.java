package io.viktoriadb;

import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemoryLayouts;
import jdk.incubator.foreign.MemorySegment;

import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.RandomAccess;

final class BTreePage extends Page {
    static final String ELEMENTS = "elements";
    private static final String LEAF_ELEMENTS = "leafElements";
    private static final String BRANCH_ELEMENTS = "branchElements";

    static final MemoryLayout LAYOUT = MemoryLayout.structLayout(
            Page.LAYOUT,
            MemoryLayouts.JAVA_SHORT.withName("count"),
            MemoryLayout.unionLayout(LeafPageElements.LAYOUT.withName(LEAF_ELEMENTS),
                    BranchPageElements.LAYOUT.withName(BRANCH_ELEMENTS)).withName(ELEMENTS)
    );

    static final int PAGE_HEADER_SIZE =
            (int) LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(ELEMENTS));


    private static final long COUNT_HANDLE_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("count"));
    private static final VarHandle COUNT_HANDLE = MemoryHandles.varHandle(short.class, ByteOrder.nativeOrder());

    private static final long BRANCH_ELEMENTS_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(ELEMENTS),
                    MemoryLayout.PathElement.groupElement(BRANCH_ELEMENTS));

    private static final long LEAF_ELEMENTS_OFFSET =
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(ELEMENTS),
                    MemoryLayout.PathElement.groupElement(LEAF_ELEMENTS));

    BTreePage(MemorySegment memorySegment) {
        super(memorySegment);
    }

    @Override
    void init() {
        super.init();
        setCount((short) 0);
    }

    short getCount() {
        return (short) COUNT_HANDLE.get(pageSegment, COUNT_HANDLE_OFFSET);
    }

    void setCount(short count) {
        COUNT_HANDLE.set(pageSegment, COUNT_HANDLE_OFFSET, count);
    }

    LeafPageElements getLeafElements() {
        final short size = (short) COUNT_HANDLE.get(pageSegment, COUNT_HANDLE_OFFSET);
        final MemorySegment memorySegment = pageSegment.asSlice(LEAF_ELEMENTS_OFFSET);
        return new LeafPageElements(memorySegment, size);
    }

    int getLeafElementOffset(int index) {
        final long offset = LEAF_ELEMENTS_OFFSET + LeafPageElement.SIZE * (long) index;
        return (int) offset;
    }

    LeafPageElement getLeafElement(int index) {
        final int offset = getLeafElementOffset(index);
        final MemorySegment memorySegment = pageSegment.asSlice(offset);

        return new LeafPageElement(memorySegment);
    }

    BranchPageElements getBranchElements() {
        final short count = (short) COUNT_HANDLE.get(pageSegment, COUNT_HANDLE_OFFSET);
        final MemorySegment memorySegment = pageSegment.asSlice(BRANCH_ELEMENTS_OFFSET);
        return new BranchPageElements(memorySegment, count);
    }

    int getBranchElementOffset(int index) {
        final long offset = BRANCH_ELEMENTS_OFFSET + ((long) index) * BranchPageElement.SIZE;
        return (int) offset;
    }

    BranchPageElement getBranchElement(int index) {
        final int offset = getBranchElementOffset(index);
        final MemorySegment memorySegment = pageSegment.asSlice(offset);

        return new BranchPageElement(memorySegment);
    }

    static final class LeafPageElement {
        private static final MemoryLayout LAYOUT = MemoryLayout.structLayout(
                MemoryLayouts.JAVA_INT.withName("flags"),
                MemoryLayouts.JAVA_INT.withName("pos"),
                MemoryLayouts.JAVA_INT.withName("ksize"),
                MemoryLayouts.JAVA_INT.withName("vsize")
        );

        static final int SIZE = (int) LAYOUT.byteSize();

        private static final long FLAGS_HANDLE_OFFSET =
                LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("flags"));
        private static final VarHandle FLAGS_HANDLE =
                MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());
        private static final long POS_HANDLE_OFFSET =
                LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("pos"));
        private static final VarHandle POS_HANDLE =
                MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

        private static final long KSIZE_HANDLE_OFFSET =
                LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("ksize"));
        private static final VarHandle KSIZE_HANDLE =
                MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

        private static final long VSIZE_HANDLE_OFFSET =
                LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("vsize"));

        private static final VarHandle VSIZE_HANDLE =
                MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());


        private final MemorySegment memorySegment;

        LeafPageElement(MemorySegment memorySegment) {
            this.memorySegment = memorySegment;
        }

        int getFlags() {
            return (int) FLAGS_HANDLE.get(memorySegment, FLAGS_HANDLE_OFFSET);
        }

        void setFlags(int flags) {
            FLAGS_HANDLE.set(memorySegment, FLAGS_HANDLE_OFFSET, flags);
        }

        void setPos(int pos) {
            POS_HANDLE.set(memorySegment, POS_HANDLE_OFFSET, pos);
        }

        int getPos() {
            return (int) POS_HANDLE.get(memorySegment, POS_HANDLE_OFFSET);
        }

        void setKSize(int ksize) {
            KSIZE_HANDLE.set(memorySegment, KSIZE_HANDLE_OFFSET, ksize);
        }

        int getKSize() {
            return (int) KSIZE_HANDLE.get(memorySegment, KSIZE_HANDLE_OFFSET);
        }


        void setVSize(int vsize) {
            VSIZE_HANDLE.set(memorySegment, VSIZE_HANDLE_OFFSET, vsize);
        }

        int getVSize() {
            return (int) VSIZE_HANDLE.get(memorySegment, VSIZE_HANDLE_OFFSET);
        }

        MemorySegment getKey() {
            final int pos = (int) POS_HANDLE.get(memorySegment, POS_HANDLE_OFFSET);
            final int ksize = (int) KSIZE_HANDLE.get(memorySegment, KSIZE_HANDLE_OFFSET);

            return memorySegment.asSlice(pos, ksize);
        }

        MemorySegment getValue() {
            final int pos = (int) POS_HANDLE.get(memorySegment, POS_HANDLE_OFFSET);
            final int ksize = (int) KSIZE_HANDLE.get(memorySegment, KSIZE_HANDLE_OFFSET);
            final int vsize = (int) VSIZE_HANDLE.get(memorySegment, VSIZE_HANDLE_OFFSET);

            return memorySegment.asSlice(pos + ksize, vsize);
        }
    }

    static final class LeafPageElements extends AbstractList<LeafPageElement> implements RandomAccess {
        private static final MemoryLayout LAYOUT = MemoryLayout.sequenceLayout(LeafPageElement.LAYOUT);
        private static final long BASE_OFFSET = LAYOUT.byteOffset(MemoryLayout.PathElement.sequenceElement(0));

        private final MemorySegment memorySegment;
        private final int size;

        public LeafPageElements(MemorySegment memorySegment, int size) {
            this.memorySegment = memorySegment;
            this.size = size;
        }

        public LeafPageElement get(final int index) {
            final long offset = BASE_OFFSET + ((long) index) * LeafPageElement.SIZE;
            final MemorySegment memorySegment = this.memorySegment.asSlice(offset);
            return new LeafPageElement(memorySegment);
        }

        @Override
        public int size() {
            return size;
        }
    }

    static final class BranchPageElement {
        private static final MemoryLayout LAYOUT = MemoryLayout.structLayout(
                MemoryLayouts.JAVA_LONG.withName("pgid"),
                MemoryLayouts.JAVA_INT.withName("ksize"),
                MemoryLayouts.JAVA_INT.withName("pos")
        );

        static final int SIZE = (int) LAYOUT.byteSize();

        private static final long PGID_HANDLE_OFFSET =
                LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("pgid"));
        private static final VarHandle PGID_HANDLE =
                MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());

        private static final long KSIZE_HANDLE_OFFSET =
                LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("ksize"));
        private static final VarHandle KSIZE_HANDLE =
                MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

        private static final long POS_HANDLE_OFFSET =
                LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("pos"));
        private static final VarHandle POS_HANDLE =
                MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

        private final MemorySegment memorySegment;

        BranchPageElement(MemorySegment memorySegment) {
            this.memorySegment = memorySegment;
        }

        long getPageId() {
            return (long) PGID_HANDLE.get(memorySegment, PGID_HANDLE_OFFSET);
        }

        void setPageId(long pageId) {
            PGID_HANDLE.set(memorySegment, PGID_HANDLE_OFFSET, pageId);
        }

        void setKSize(int ksize) {
            KSIZE_HANDLE.set(memorySegment, KSIZE_HANDLE_OFFSET, ksize);
        }

        int getKSize() {
            return (int) KSIZE_HANDLE.get(memorySegment, KSIZE_HANDLE_OFFSET);
        }

        void setPos(int pos) {
            POS_HANDLE.set(memorySegment, POS_HANDLE_OFFSET, pos);
        }

        int getPos() {
            return (int) POS_HANDLE.get(memorySegment, POS_HANDLE_OFFSET);
        }

        MemorySegment getKey() {
            final int pos = (int) POS_HANDLE.get(memorySegment, POS_HANDLE_OFFSET);
            final int ksize = (int) KSIZE_HANDLE.get(memorySegment, KSIZE_HANDLE_OFFSET);

            return memorySegment.asSlice(pos, ksize);
        }
    }

    static final class BranchPageElements extends AbstractList<BranchPageElement> implements RandomAccess {
        private static final MemoryLayout LAYOUT = MemoryLayout.sequenceLayout(BranchPageElement.LAYOUT);
        private static final long BASE_OFFSET = LAYOUT.byteOffset(MemoryLayout.PathElement.sequenceElement(0));

        private final MemorySegment memorySegment;
        private final int size;


        BranchPageElements(MemorySegment memorySegment, int size) {
            this.memorySegment = memorySegment;
            this.size = size;
        }

        public BranchPageElement get(final int index) {
            final long offset = BASE_OFFSET + ((long) index) * BranchPageElement.SIZE;
            final MemorySegment memorySegment = this.memorySegment.asSlice(offset);
            return new BranchPageElement(memorySegment);
        }

        @Override
        public int size() {
            return size;
        }
    }
}



