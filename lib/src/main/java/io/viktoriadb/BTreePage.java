package io.viktoriadb;

import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemoryLayouts;
import jdk.incubator.foreign.MemorySegment;

import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
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


    private static final VarHandle COUNT_HANDLE = LAYOUT.varHandle(short.class,
            MemoryLayout.PathElement.groupElement("count"));

    BTreePage(MemorySegment memorySegment) {
        super(memorySegment);
    }

    short getCount() {
        return (short) COUNT_HANDLE.get(pageSegment);
    }

    void setCount(short count) {
        COUNT_HANDLE.set(pageSegment, count);
    }

    LeafPageElements getLeafElements() {
        final short size = (short) COUNT_HANDLE.get(pageSegment);
        final long offset = LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(ELEMENTS),
                MemoryLayout.PathElement.groupElement(LEAF_ELEMENTS));
        final MemorySegment memorySegment = pageSegment.asSlice(offset);
        return new LeafPageElements(memorySegment, size);
    }

    int getLeafElementOffset(int index) {
        final long offset = LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(ELEMENTS),
                MemoryLayout.PathElement.groupElement(LEAF_ELEMENTS),
                MemoryLayout.PathElement.sequenceElement(index));
        return (int) offset;
    }

    LeafPageElement getLeafElement(int index) {
        final int offset = getLeafElementOffset(index);
        final MemorySegment memorySegment = pageSegment.asSlice(offset);

        return new LeafPageElement(memorySegment);
    }

    BranchPageElements getBranchElements() {
        final short count = (short) COUNT_HANDLE.get(pageSegment);
        final long offset = LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(ELEMENTS),
                MemoryLayout.PathElement.groupElement(BRANCH_ELEMENTS));
        final MemorySegment memorySegment = pageSegment.asSlice(offset);
        return new BranchPageElements(memorySegment, count);
    }

    int getBranchElementOffset(int index) {
        final long offset = LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(ELEMENTS),
                MemoryLayout.PathElement.groupElement(BRANCH_ELEMENTS),
                MemoryLayout.PathElement.sequenceElement(index));
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

        private static final VarHandle FLAGS_HANDLE =
                LAYOUT.varHandle(int.class,
                        MemoryLayout.PathElement.groupElement("flags"));
        private static final VarHandle POS_HANDLE =
                LAYOUT.varHandle(int.class,
                        MemoryLayout.PathElement.groupElement("pos"));
        private static final VarHandle KSIZE_HANDLE =
                LAYOUT.varHandle(int.class,
                        MemoryLayout.PathElement.groupElement("ksize"));
        private static final VarHandle VSIZE_HANDLE =
                LAYOUT.varHandle(int.class,
                        MemoryLayout.PathElement.groupElement("vsize"));


        private final MemorySegment memorySegment;

        LeafPageElement(MemorySegment memorySegment) {
            this.memorySegment = memorySegment;
        }

        int getFlags() {
            return (int) FLAGS_HANDLE.get(memorySegment);
        }

        void setFlags(int flags) {
            FLAGS_HANDLE.set(memorySegment, flags);
        }

        void setPos(int pos) {
            POS_HANDLE.set(memorySegment, pos);
        }

        int getPos() {
            return (int) POS_HANDLE.get(memorySegment);
        }

        void setKSize(int ksize) {
            KSIZE_HANDLE.set(memorySegment, ksize);
        }

        int getKSize() {
            return (int) KSIZE_HANDLE.get(memorySegment);
        }


        void setVSize(int vsize) {
            VSIZE_HANDLE.set(memorySegment, vsize);
        }

        int getVSize() {
            return (int) VSIZE_HANDLE.get(memorySegment);
        }

        ByteBuffer getKey() {
            final int pos = (int) POS_HANDLE.get(memorySegment);
            final int ksize = (int) KSIZE_HANDLE.get(memorySegment);

            return memorySegment.asSlice(pos, ksize).asByteBuffer();
        }

        ByteBuffer getValue() {
            final int pos = (int) POS_HANDLE.get(memorySegment);
            final int ksize = (int) KSIZE_HANDLE.get(memorySegment);
            final int vsize = (int) VSIZE_HANDLE.get(memorySegment);

            return memorySegment.asSlice(pos + ksize, vsize).asByteBuffer();
        }
    }

    static final class LeafPageElements extends AbstractList<LeafPageElement> implements RandomAccess {
        private static final MemoryLayout LAYOUT = MemoryLayout.sequenceLayout(LeafPageElement.LAYOUT);
        private final MemorySegment memorySegment;
        private final int size;

        public LeafPageElements(MemorySegment memorySegment, int size) {
            this.memorySegment = memorySegment;
            this.size = size;
        }

        public LeafPageElement get(final int index) {
            final long offset = LAYOUT.byteOffset(MemoryLayout.PathElement.sequenceElement(index));
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

        private static final VarHandle PGID_HANDLE =
                LAYOUT.varHandle(long.class, MemoryLayout.PathElement.groupElement("pgid"));
        private static final VarHandle KSIZE_HANDLE =
                LAYOUT.varHandle(int.class, MemoryLayout.PathElement.groupElement("ksize"));
        private static final VarHandle POS_HANDLE =
                LAYOUT.varHandle(int.class, MemoryLayout.PathElement.groupElement("pos"));

        private final MemorySegment memorySegment;

        BranchPageElement(MemorySegment memorySegment) {
            this.memorySegment = memorySegment;
        }

        long getPageId() {
            return (long) PGID_HANDLE.get(memorySegment);
        }

        void setPageId(long pageId) {
            PGID_HANDLE.set(memorySegment, pageId);
        }

        void setKSize(int ksize) {
            KSIZE_HANDLE.set(memorySegment, ksize);
        }

        int getKSize() {
            return (int) KSIZE_HANDLE.get(memorySegment);
        }

        void setPos(int pos) {
            POS_HANDLE.set(memorySegment, pos);
        }

        int getPos() {
            return (int) POS_HANDLE.get(memorySegment);
        }

        ByteBuffer getKey() {
            final int pos = (int) POS_HANDLE.get(memorySegment);
            final int ksize = (int) KSIZE_HANDLE.get(memorySegment);

            return memorySegment.asSlice(pos, ksize).asByteBuffer();
        }
    }

    static final class BranchPageElements extends AbstractList<BranchPageElement> implements RandomAccess {
        private static final MemoryLayout LAYOUT = MemoryLayout.sequenceLayout(BranchPageElement.LAYOUT);
        private final MemorySegment memorySegment;
        private final int size;

        BranchPageElements(MemorySegment memorySegment, int size) {
            this.memorySegment = memorySegment;
            this.size = size;
        }

        public BranchPageElement get(final int index) {
            final long offset = LAYOUT.byteOffset(MemoryLayout.PathElement.sequenceElement(index));
            final MemorySegment memorySegment = this.memorySegment.asSlice(offset);
            return new BranchPageElement(memorySegment);
        }

        @Override
        public int size() {
            return size;
        }
    }
}


