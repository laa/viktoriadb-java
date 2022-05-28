package io.viktoriadb;

import com.google.common.collect.Lists;
import io.viktoriadb.exceptions.DbException;
import io.viktoriadb.util.ByteBufferComparator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jdk.incubator.foreign.MemoryLayout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;

/**
 * Represents an in-memory, deserialized page.
 * <p>
 * Implementation note: All keys and values may be backed by the underlying mmap.
 */
final class Node {
    @SuppressWarnings("Guava")
    private static final com.google.common.base.Function<Node.INode, ByteBuffer>
            iNodeKeyViewTransformer = inode -> inode.key;

    Bucket bucket;
    boolean isLeaf;
    boolean unbalanced;
    boolean spilled;
    ByteBuffer firstKey;
    long pageId;
    Node parent;
    ArrayList<Node> children = new ArrayList<>();
    ObjectArrayList<INode> inodes = new ObjectArrayList<>();

    /**
     * @return the top-level node this node is attached to.
     */
    Node root() {
        if (parent == null) {
            return this;
        }

        return parent.root();
    }

    /**
     * @return minimum number of inodes this node should have.
     */
    int minKeys() {
        if (isLeaf) {
            return 1;
        }
        return 2;
    }

    /**
     * @return the size of the node after serialization.
     */
    int size() {
        final int elementSize = pageElementSize();
        int size = BTreePage.PAGE_HEADER_SIZE;

        final boolean isLeaf = this.isLeaf;

        for (var iNode : inodes) {
            size += elementSize + iNode.key.remaining();

            if (isLeaf) {
                size += iNode.value.remaining();
            }
        }

        return size;
    }

    /**
     * true if the node is less than a given size.
     * This is an optimization to avoid calculating a large node when we only need
     * to know if it fits inside a certain page size.
     *
     * @param givenSize Size to compare
     * @return true if the node is less than a given size.
     */
    private boolean sizeLessThan(int givenSize) {
        final int elementSize = pageElementSize();
        int size = BTreePage.PAGE_HEADER_SIZE;

        final boolean isLeaf = this.isLeaf;

        for (var iNode : inodes) {
            size += elementSize + iNode.key.remaining();

            if (isLeaf) {
                size += iNode.value.remaining();
            }

            if (size >= givenSize) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return the number of children.
     */
    int numChildren() {
        return inodes.size();
    }

    /**
     * @return the next node with the same parent.
     */
    Node nextSibling() {
        if (parent == null) {
            return null;
        }

        var index = parent.childIndex(this);
        return parent.childAt(index + 1);
    }

    /**
     * @return the previous node with the same parent.
     */
    Node prevSibling() {
        if (parent == null) {
            return null;
        }

        var index = parent.childIndex(this);
        return parent.childAt(index - 1);
    }

    /**
     * Inserts a key/value into the current node alongside with metadata.
     * That is universal method and as result some values may be not stored if they do not have meaning for the
     * given type of node.
     *
     * @param oldKey Key which needs to be removed, use value of new key if you do not want to remove old key.
     * @param newKey Value of key which should be inserted instead of old key
     * @param value  Value associated with new key, will be taken into account only for leaf nodes.
     * @param pageId ID of the child page.
     * @param flags  Flags stored in a leaf element, will be taken into account only for the leaf node.
     */
    void put(ByteBuffer oldKey, ByteBuffer newKey, ByteBuffer value, long pageId, int flags) {
        if (pageId >= bucket.tx.meta.getMaxPageId()) {
            throw new DbException(
                    String.format("pgid (%d) above high water mark (%d)", pageId, bucket.tx.meta.getMaxPageId()));
        } else if (oldKey == null || oldKey.remaining() == 0) {
            throw new DbException("put: zero-length old key");
        } else if (newKey == null || newKey.remaining() == 0) {
            throw new DbException("put: zero-length new key");
        }

        assert newKey.remaining() > 0 : "put: zero-length inode key";

        @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
        var iNodeKeyView = Lists.transform(inodes, iNodeKeyViewTransformer);
        int index = Collections.binarySearch(iNodeKeyView, oldKey, ByteBufferComparator.INSTANCE);

        INode inode;
        if (index < 0) {
            inode = new INode();
            inodes.add(-index - 1, inode);
        } else {
            inode = inodes.get(index);
        }

        inode.key = newKey;
        inode.value = value;
        inode.pageId = pageId;
        inode.flags = flags;
    }

    void read(BTreePage page) {
        pageId = page.getPageId();
        isLeaf = (page.getFlags() & Page.LEAF_PAGE_FLAG) != 0;
        final int count = page.getCount();
        inodes = new ObjectArrayList<>(count);

        final boolean isLeaf = this.isLeaf;
        for (int i = 0; i < count; i++) {
            var inode = new INode();

            if (isLeaf) {
                var elem = page.getLeafElement(i);
                inode.flags = elem.getFlags();
                inode.key = elem.getKey();
                inode.value = elem.getValue();
            } else {
                var elem = page.getBranchElement(i);
                inode.pageId = elem.getPageId();
                inode.key = elem.getKey();
            }

            assert inode.key.remaining() > 0 : "read: zero-length inode key";
            inodes.add(inode);
        }

        // Save first key so we can find the node in the parent when we spill.
        if (!inodes.isEmpty()) {
            firstKey = inodes.get(0).key;
            assert firstKey.remaining() > 0 : "read: zero-length node key";
        } else {
            firstKey = null;
        }
    }

    /**
     * Writes the items onto one or more pages.
     *
     * @param page page/chunk of pages which will contain content of the node
     */
    void write(BTreePage page) {
        if (isLeaf) {
            page.setFlags((short) (page.getFlags() | Page.LEAF_PAGE_FLAG));
        } else {
            page.setFlags((short) (page.getFlags() | Page.BRANCH_PAGE_FLAG));
        }

        if (inodes.size() >= 0xFFFF) {
            throw new DbException(String.format("inode overflow: %d (pgid=%d)", inodes.size(), pageId));
        }

        page.setCount((short) inodes.size());

        if (inodes.isEmpty()) {
            return;
        }

        int entriesOffset = (int)
                BTreePage.LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(BTreePage.ELEMENTS));
        if (isLeaf) {
            entriesOffset += inodes.size() * BTreePage.LeafPageElement.SIZE;
        } else {
            entriesOffset += inodes.size() * BTreePage.BranchPageElement.SIZE;
        }

        final ByteBuffer buffer = page.pageSegment.asSlice(entriesOffset).asByteBuffer();

        //give jit a chance to optimize loop by avoiding branching
        final boolean isLeaf = this.isLeaf;

        for (int i = 0; i < inodes.size(); i++) {
            var inode = inodes.get(i);

            assert inode.key.remaining() > 0 : "write: zero-length inode key";

            if (isLeaf) {
                var elem = page.getLeafElement(i);
                int leafElementOffset = page.getLeafElementOffset(i);
                elem.setPos(entriesOffset + buffer.position() - leafElementOffset);

                elem.setFlags(inode.flags);
                elem.setKSize(inode.key.remaining());
                elem.setVSize(inode.value.remaining());
            } else {
                var elem = page.getBranchElement(i);
                int branchElementOffset = page.getBranchElementOffset(i);

                elem.setPos(entriesOffset + buffer.position() - branchElementOffset);
                elem.setKSize(inode.key.remaining());
                elem.setPageId(inode.pageId);

                assert inode.pageId != pageId : "write: circular dependency occurred";
            }

            int position = inode.key.position();
            buffer.put(inode.key);
            inode.key.position(position);

            if (isLeaf) {
                position = inode.value.position();
                buffer.put(inode.value);
                inode.value.position(position);
            }
        }
    }

    /**
     * Writes the nodes to dirty pages and splits nodes as it goes.
     *
     * @throws DbException If dirty pages can not be allocated.
     */
    void spill() {
        if (spilled) {
            return;
        }

        //if this node is bucket root node
        boolean wasRoot = bucket.rootNode == this;

        var tx = bucket.tx;
        // Spill child nodes first. Child nodes can materialize sibling nodes in
        // the case of split-merge. We have to check
        // the children size on every loop iteration.
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < children.size(); i++) {
            children.get(i).spill();
        }

        //we do not need old children anymore
        children.clear();

        // Split nodes into appropriate sizes. The first node will always be current node.
        var nodes = split(tx.db.pageSize);

        if (wasRoot && nodes.size() > 1) {
            //update root node of the bucket, otherwise remmaping of mmap can skip key during call of dereference
            //and key/value will reference to invalid memory address
            bucket.rootNode = root();
        }

        for (var node : nodes) {
            //assert parent == null || parent.childIndex(node) >= 0;

            // Add node's page to the freelist if it's not new.
            if (node.pageId > 0) {
                tx.db.freeList.free(tx.meta.getTXId(), tx.page(node.pageId));
                node.pageId = 0;
            }

            // Allocate contiguous space for the node.
            var page = (BTreePage) tx.allocate((node.size() / tx.db.pageSize) + 1,
                    node.isLeaf ? Page.PageType.LEAF_PAGE : Page.PageType.BRANCH_PAGE);

            if (page.getPageId() >= tx.meta.getMaxPageId()) {
                throw new DbException(String.format("pgid (%d) above high water mark (%d)", page.getPageId(),
                        tx.meta.getMaxPageId()));
            }

            node.pageId = page.getPageId();
            node.write(page);
            node.spilled = true;

            if (node.parent != null) {
                var key = node.firstKey;
                if (key == null) {
                    key = node.inodes.get(0).key;
                }

                //new key can be inserted at the begging of the node so we need to replace/add it in parent
                //references
                node.parent.put(key, node.inodes.get(0).key, null, node.pageId, 0);
                node.firstKey = node.inodes.get(0).key;
                assert node.firstKey.remaining() > 0 : "spill: zero-length node key";
            }

            tx.stats.spill++;
        }

        // If the root node split and created a new root then we need to spill that
        // as well.
        if (parent != null && parent.pageId == 0) {
            parent.spill();
        }
    }

    void rebalance() {
        if (!unbalanced) {
            return;
        }

        unbalanced = false;
        bucket.tx.stats.rebalance++;

        // Ignore if node is above threshold (25%) and has enough keys.
        var threshold = bucket.tx.db.pageSize / 4;
        if (size() > threshold && inodes.size() > minKeys()) {
            return;
        }

        // Root node has special handling.
        if (parent == null) {
            // If root node is a branch and only has one node then collapse it.
            if (!isLeaf && inodes.size() == 1) {
                // Move root's child up.
                var child = bucket.node(inodes.get(0).pageId, this);
                isLeaf = child.isLeaf;
                inodes = child.inodes;
                children = child.children;

                // Reparent all child nodes being moved.
                for (var node : inodes) {
                    var ch = bucket.nodes.get(node.pageId);
                    if (ch != null) {
                        ch.parent = this;
                    }
                }

                // Remove old child.
                child.parent = null;
                bucket.nodes.remove(child.pageId);
                child.free();
            }

            return;
        }

        // If node has no keys then just remove it.
        if (numChildren() == 0) {
            parent.del(firstKey);
            parent.removeChild(this);
            bucket.nodes.remove(pageId);
            free();
            parent.rebalance();
            return;
        }

        assert parent.numChildren() > 1 : "parent must have at least 2 children";

        // Destination node is right sibling if idx == 0, otherwise left sibling.
        // If both this node and the target node are too small then merge them.

        boolean useNextSibling = parent.childIndex(this) == 0;
        if (useNextSibling) {
            var targetNode = nextSibling();
            Objects.requireNonNull(targetNode);

            for (var inode : targetNode.inodes) {
                var ch = bucket.nodes.get(inode.pageId);
                if (ch != null) {
                    ch.parent = this;
                    children.add(ch);
                }
            }

            inodes.addAll(targetNode.inodes);
            parent.del(targetNode.firstKey);
            parent.removeChild(targetNode);

            bucket.nodes.remove(targetNode.pageId);
            targetNode.free();
        } else {
            var targetNode = prevSibling();
            Objects.requireNonNull(targetNode);

            for (var inode : inodes) {
                var ch = bucket.nodes.get(inode.pageId);
                if (ch != null) {
                    ch.parent = targetNode;
                    targetNode.children.add(ch);
                }
            }

            targetNode.inodes.addAll(inodes);
            parent.del(firstKey);
            parent.removeChild(this);
            bucket.nodes.remove(pageId);

            free();
        }

        parent.rebalance();
    }

    /**
     * Removes a node from the list of in-memory children.
     * This does not affect the inodes.
     *
     * @param node Node to be removed.
     */
    private void removeChild(Node node) {
        children.remove(node);
    }

    /**
     * Adds the node's underlying page to the freelist.
     */
    void free() {
        if (pageId > 0) {
            bucket.tx.db.freeList.free(bucket.tx.meta.getTXId(), bucket.tx.page(pageId));
            pageId = 0;
        }
    }

    /**
     * Causes the node to copy all its inode key/value references to heap memory.
     * This is required when the mmap is reallocated so inodes are not pointing to stale data.
     */
    void dereference() {
        if (firstKey != null) {
            this.firstKey = copyToHeapBuffer(this.firstKey);
            assert pageId == 0 || firstKey.remaining() > 0 : "dereference: zero-length node key on existing node";
        }

        for (var inode : inodes) {
            inode.key = copyToHeapBuffer(inode.key);
            assert inode.key.remaining() > 0 : "dereference: zero-length inode key";

            if (isLeaf) {
                inode.value = copyToHeapBuffer(inode.value);
            }
        }

        for (var node : children) {
            node.dereference();
        }

        bucket.tx.stats.nodeDeref++;
    }

    /**
     * Copies content of any passed in ByteBuffer to the heap based ByteBuffer if needed.
     *
     * @param source Buffer needs to be copied.
     * @return Copy of ByteBuffer, or the same ByteBuffer as was passed if it is based on heap.
     */
    private static ByteBuffer copyToHeapBuffer(final ByteBuffer source) {
        if (source == null) {
            return source;
        }

        if (!source.isDirect()) {
            return source;
        }

        final byte[] data = new byte[source.remaining()];
        source.get(source.position(), data);
        return ByteBuffer.wrap(data);
    }

    /**
     * Breaks up a node into multiple smaller nodes, if appropriate.
     * This should only be called from the {@link Node#spill()} function.
     *
     * @param pageSize Page size limit. That is soft limit, it is satisfied if node will contain at least minimum
     *                 amount of keys.
     * @return List of nodes, created during split. First index always contains current node.
     */
    ArrayList<Node> split(int pageSize) {
        ArrayList<Node> nodes = new ArrayList<>();

        var node = this;
        while (true) {
            // Split node into two.
            var newNode = node.splitTwo(pageSize);
            nodes.add(node);

            // If we can't split then exit the loop.
            if (newNode == null) {
                break;
            }

            // Set node to newNode so it gets split on the next iteration.
            node = newNode;
        }

        return nodes;
    }

    /**
     * Breaks up a node into two smaller nodes, if appropriate.
     * This should only be called from the split() function.
     *
     * @param pageSize Requested page size, it is soft limit and may be not satisfied.
     * @return New node if current node is split, or null otherwise.
     */
    private Node splitTwo(int pageSize) {
        // Ignore the split if the page doesn't have at least enough nodes for
        // two pages or if the nodes can fit in a single page.
        if (inodes.size() < 2 * minKeys() || sizeLessThan(pageSize)) {
            return null;
        }

        var fillPercent = bucket.fillPercent;
        if (fillPercent < Bucket.MIN_FILL_PERCENT) {
            fillPercent = Bucket.MIN_FILL_PERCENT;
        } else if (fillPercent > Bucket.MAX_FILL_PERCENT) {
            fillPercent = Bucket.MAX_FILL_PERCENT;
        }

        int threshold = (int) (pageSize * fillPercent);
        int splitIndex = splitIndex(threshold);


        // Split node into two separate nodes.
        // If there's no parent then we'll need to create one.
        if (parent == null) {
            parent = new Node();
            parent.bucket = bucket;
            parent.children = new ArrayList<>();
            parent.children.add(this);
        }

        // Create a new node and add it to the parent.
        var next = new Node();
        next.bucket = bucket;
        next.isLeaf = isLeaf;
        next.parent = parent;

        next.parent.children.add(next);

        next.inodes = new ObjectArrayList<>(inodes.subList(splitIndex, inodes.size()));
        next.firstKey = next.inodes.get(0).key;

        inodes.removeElements(splitIndex, inodes.size());

        // Update the statistics.
        bucket.tx.stats.split++;
        return next;
    }

    /**
     * Finds the position where a page will fill a given threshold.
     * This is only be called from split().
     *
     * @param threshold Requested threshold.
     * @return the index of the first page.
     */
    private int splitIndex(int threshold) {
        int sz = BTreePage.PAGE_HEADER_SIZE;

        int index = 0;
        final int pageElementSize = pageElementSize();
        final boolean isLeaf = this.isLeaf;
        final int minKeys = minKeys();

        assert inodes.size() >= 2 * minKeys;
        // Loop until we only have the minimum number of keys required for the second page.
        for (int i = 0; i < inodes.size() - minKeys; i++) {
            index = i;
            var inode = inodes.get(i);

            int elsize = pageElementSize + inode.key.remaining();
            if (isLeaf) {
                elsize += inode.value.remaining();
            }

            sz += elsize;
            // If we have at least the minimum number of keys and adding another
            // node would put us over the threshold then exit and return.
            if (i >= minKeys && sz > threshold) {
                return index;
            }
        }

        //no pages left on first page, then keep minimum amount of keys
        if (index == 0) {
            index = minKeys;
        }
        return index;
    }


    /**
     * @param child Child index of which to be found.
     * @return the index of a given child node.
     */
    private int childIndex(Node child) {
        @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
        var iNodeKeyView = Lists.transform(inodes, iNodeKeyViewTransformer);
        return Collections.binarySearch(iNodeKeyView, child.firstKey, ByteBufferComparator.INSTANCE);
    }

    /**
     * @return the size of each page element based on the type of node.
     */
    private int pageElementSize() {
        if (isLeaf) {
            return BTreePage.LeafPageElement.SIZE;
        }

        return BTreePage.BranchPageElement.SIZE;
    }

    /**
     * Removes a key from the node.
     *
     * @param key key to remove.
     */
    void del(ByteBuffer key) {
        @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
        var iNodesKeyView = Lists.transform(inodes, iNodeKeyViewTransformer);
        int index = Collections.binarySearch(iNodesKeyView, key, ByteBufferComparator.INSTANCE);

        if (index < 0) {
            return;
        }

        inodes.remove(index);
        unbalanced = true;
    }

    boolean isLeaf() {
        return isLeaf;
    }

    /**
     * @param index Child index.
     * @return the child node at a given index.
     */
    Node childAt(int index) {
        if (isLeaf) {
            throw new DbException(String.format("invalid childAt(%d) on a leaf node", index));
        }

        if (index < 0 || index >= inodes.size()) {
            return null;
        }

        return bucket.node(inodes.get(index).pageId, this);
    }

    /**
     * Represents an internal node inside of a node.
     * It can be used to point to elements in a page or point
     * to an element which hasn't been added to a page yet.
     * <p>
     * Implementation note: All keys and values may be backed by the underlying mmap.
     */
    static final class INode {
        int flags;
        long pageId;
        ByteBuffer key;
        ByteBuffer value;
    }

    static final class NodeKeyComparator implements Comparator<Node> {
        static final NodeKeyComparator INSTANCE = new NodeKeyComparator();

        @Override
        public int compare(Node firstNode, Node secondNode) {
            return ByteBufferComparator.INSTANCE.compare(firstNode.firstKey, secondNode.firstKey);
        }
    }
}
