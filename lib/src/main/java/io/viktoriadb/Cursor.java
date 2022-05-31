package io.viktoriadb;

import com.google.common.collect.Lists;
import io.viktoriadb.exceptions.*;
import io.viktoriadb.util.MemorySegmentComparator;
import io.viktoriadb.util.ObjectObjectIntTriple;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jdk.incubator.foreign.MemorySegment;

import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * Cursor represents an iterator that can traverse over all key/value pairs in a bucket in sorted order.
 * Cursors see nested buckets with value == null.
 * Cursors can be obtained from a transaction and are valid as long as the transaction is open.
 * <p>
 * Keys and values returned from the cursor are only valid for the life of the transaction.
 * <p>
 * Changing data while traversing with a cursor may cause it to be invalidated
 * and return unexpected keys and/or values. You must reposition your cursor
 * after mutating data.
 */
public final class Cursor {
    @SuppressWarnings("Guava")
    private static final com.google.common.base.Function<Node.INode, MemorySegment>
            iNodeKeyViewTransformer = inode -> inode.key;

    @SuppressWarnings("Guava")
    private static final com.google.common.base.Function<BTreePage.LeafPageElement, MemorySegment>
            leafPageKeyViewTransformer = BTreePage.LeafPageElement::getKey;

    @SuppressWarnings("Guava")
    private static final com.google.common.base.Function<BTreePage.BranchPageElement, MemorySegment>
            branchPageKeyViewTransformer = BTreePage.BranchPageElement::getKey;


    private final Bucket bucket;
    private final ObjectArrayList<ElemRef> stack;

    /**
     * This can be set during call to the delete method, because in such case cursor can be moved to the next
     * element.
     */
    private boolean skipNext;

    /**
     * Indicates that item which is pointed by cursor already deleted.
     */
    private boolean currentItemDeleted;

    Cursor(Bucket bucket) {
        this.bucket = bucket;
        stack = new ObjectArrayList<>();
    }

    /**
     * @return the bucket that this cursor was created from.
     */
    public Bucket bucket() {
        return bucket;
    }

    /**
     * Moves the cursor to the first item in the bucket and returns its key and value.
     * If the bucket is empty then a null is returned.
     * The returned key and value are only valid for the life of the transaction.
     *
     * @return Smallest key and value pair in the bucket.
     */
    public ByteBuffer[] first() {
        assert bucket.tx.db != null : "tx is closed";

        skipNext = false;
        currentItemDeleted = false;

        stack.clear();
        var pageNode = bucket.pageNode(bucket.root);
        stack.add(new ElemRef(pageNode.first(), pageNode.second()));

        doFirst();

        // If we land on an empty page then move to the next value.
        if (stack.get(stack.size() - 1).count() == 0) {
            doNext();
        }

        var kvFlags = keyValue();
        if (kvFlags == null) {
            return null;
        }

        return convertToKVPair(kvFlags);
    }

    /**
     * Moves the cursor to the last item in the bucket and returns its key and value.
     * If the bucket is empty then a null key and value are returned.
     * The returned key and value are only valid for the life of the transaction.
     *
     * @return Biggest key and value pair in the bucket.
     */
    public ByteBuffer[] last() {
        assert bucket.tx.db != null : "tx is closed";

        skipNext = false;
        currentItemDeleted = false;

        stack.clear();
        var pageNode = bucket.pageNode(bucket.root);
        var ref = new ElemRef(pageNode.first(), pageNode.second());
        ref.index = ref.count() - 1;

        stack.add(ref);
        doLast();

        var kvFlags = keyValue();
        if (kvFlags == null) {
            return null;
        }

        return convertToKVPair(kvFlags);
    }

    /**
     * Moves the cursor to the next item in the bucket and returns its key and value.
     * If the cursor is at the end of the bucket then a null is returned.
     * The returned key and value are only valid for the life of the transaction.
     *
     * @return Current KV pair.
     */
    public ByteBuffer[] next() {
        assert bucket.tx.db != null : "tx is closed";

        final ObjectObjectIntTriple<MemorySegment, MemorySegment> kvFlags;
        if (skipNext) {
            kvFlags = keyValue();
        } else {
            kvFlags = doNext();
        }

        skipNext = false;
        currentItemDeleted = false;

        if (kvFlags == null) {
            return null;
        }

        return convertToKVPair(kvFlags);
    }

    /**
     * Prev moves the cursor to the previous item in the bucket and returns its key and value.
     * If the cursor is at the beginning of the bucket then a null is returned.
     * The returned key and value are only valid for the life of the transaction.
     *
     * @return Current KV pair.
     */
    public ByteBuffer[] prev() {
        assert bucket.tx.db != null : "tx is closed";

        skipNext = false;
        currentItemDeleted = false;

        // Attempt to move back one element until we're successful.
        // Move up the stack as we hit the beginning of each page in our stack.

        while (!stack.isEmpty()) {
            var ref = stack.get(stack.size() - 1);
            if (ref.index > 0) {
                ref.index--;
                break;
            }

            stack.remove(stack.size() - 1);
        }

        if (stack.isEmpty()) {
            return null;
        }

        doLast();

        var kvFlags = keyValue();
        if (kvFlags == null) {
            return null;
        }

        return convertToKVPair(kvFlags);
    }

    /**
     * Moves the cursor to a given key and returns it.
     * If the key does not exist then the next key is used. If no keys
     * follow, a null is returned.
     * The returned key and value are only valid for the life of the transaction.
     *
     * @param key Requested key.
     * @return Current KV pair.
     */
    public ByteBuffer[] seek(ByteBuffer key) {
        skipNext = false;
        currentItemDeleted = false;

        var kvFlags = doSeek(MemorySegment.ofByteBuffer(key));
        if (kvFlags == null) {
            return null;
        }

        return convertToKVPair(kvFlags);
    }

    /**
     * Removes the current key/value under the cursor from the bucket.
     *
     * @throws DbException if current key/value is a bucket or if the transaction
     *                     is not writable or currently positioned item already deleted.
     */
    public void delete() {
        if (bucket.tx.db == null) {
            throw new TransactionIsClosedException();
        } else if (!bucket.writable()) {
            throw new TransactionIsNotWritableException();
        }

        var kvFlags = keyValue();
        if (kvFlags == null) {
            throw new CursorIsNotPositionedException();
        }

        if ((kvFlags.third & Bucket.BUCKET_LEAF_FLAG) != 0) {
            throw new IncompatibleValueException();
        }

        if (currentItemDeleted) {
            throw new CursorIsNotPositionedException();
        }

        var node = node();
        node.del(kvFlags.first);

        skipNext = !node.inodes.isEmpty();
        currentItemDeleted = true;
    }

    /**
     * @return Node that the cursor is currently positioned on.
     */
    Node node() {
        assert !stack.isEmpty() : "Accessing a node with empty cursor";

        //noinspection ConstantConditions
        if (stack.isEmpty()) {
            throw new DbException("Accessing a node with not positioned cursor");
        }

        // If the top of the stack is a leaf node then just return it.
        var ref = stack.get(stack.size() - 1);
        if (ref.node != null && ref.isLeaf()) {
            return ref.node;
        }

        ref = stack.get(0);
        if (ref.node == null) {
            ref.node = bucket.node(ref.page.getPageId(), null);
        }

        var node = ref.node;

        for (int i = 1; i < stack.size(); i++) {
            //noinspection ConstantConditions
            assert !node.isLeaf() : "expected branch node";

            var nextRef = stack.get(i);

            if (nextRef.node == null) {
                nextRef.node = node.childAt(ref.index);
            }

            node = nextRef.node;
            ref = nextRef;
        }

        //noinspection ConstantConditions
        assert node.isLeaf() : "expected leaf node";
        //noinspection ConstantConditions
        if (!node.isLeaf()) {
            throw new DbException("Accessing a node with not positioned cursor");
        }

        return node;
    }

    /**
     * Converts KV pair and flags into the user accessible form.
     * If this pair represents a bucket, null value will be returned instead of actual value.
     *
     * @param kvFlags KV pair and flags contained in {@link BTreePage.LeafPageElement}.
     * @return KV pair
     */
    private ByteBuffer[] convertToKVPair(ObjectObjectIntTriple<MemorySegment, MemorySegment> kvFlags) {
        if ((kvFlags.third & Bucket.BUCKET_LEAF_FLAG) != 0) {
            //copy key/value pairs, if tx is writable remmapping can invalidate buffer
            var key = unmap(kvFlags.first);
            return new ByteBuffer[]{key.asReadOnlyBuffer(), null};
        }

        var key = unmap(kvFlags.first);
        var value = unmap(kvFlags.second);
        return new ByteBuffer[]{key.asReadOnlyBuffer(), value.asReadOnlyBuffer()};
    }

    /**
     * Moves the cursor to a given key and returns it.
     * If the key does not exist then the next key is used.
     *
     * @param key Key to search
     * @return Key, value and flags of the leaf page which contains passed
     * in key or the smallest key bigger than passed in.
     */
    ObjectObjectIntTriple<MemorySegment, MemorySegment> doSeek(MemorySegment key) {
        assert bucket.tx.db != null : "Tx closed";

        // Start from root page/node and traverse to correct page.
        stack.clear();
        search(key, bucket.root);

        // If we ended up after the last element of a page then move to the next one.
        var ref = stack.get(stack.size() - 1);
        if (ref.index >= ref.count()) {
            return doNext();
        }

        return keyValue();
    }


    /**
     * Moves the cursor to the first leaf element under the last page in the stack.
     */
    private void doFirst() {
        var ref = stack.get(stack.size() - 1);
        // Exit when we hit a leaf page.
        while (!ref.isLeaf()) {
            // Keep adding pages pointing to the first element to the stack.
            long pageId;
            if (ref.node != null) {
                pageId = ref.node.inodes.get(ref.index).pageId;
            } else {
                pageId = ref.page.getBranchElement(ref.index).getPageId();
            }

            var pageNode = bucket.pageNode(pageId);
            ref = new ElemRef(pageNode.first(), pageNode.second());
            stack.add(ref);
        }
    }

    /**
     * Moves to the next leaf element and returns the key and value.
     * If the cursor is at the last leaf element then it stays there and returns null.
     *
     * @return Next key and value or null if such one does not exist.
     */
    private ObjectObjectIntTriple<MemorySegment, MemorySegment> doNext() {
        while (true) {
            int i = stack.size() - 1;
            for (; i >= 0; i--) {
                var elem = stack.get(i);
                if (elem.index < elem.count() - 1) {
                    elem.index++;
                    break;
                }
            }

            // If we've hit the root page then stop and return. This will leave the
            // cursor on the last element of the last page.
            if (i == -1) {
                return null;
            }

            // Otherwise start from where we left off in the stack and find the
            // first element of the first leaf page.
            stack.removeElements(i + 1, stack.size());
            doFirst();

            // If this is an empty page then restart and move back up the stack.
            if (stack.get(stack.size() - 1).count() == 0) {
                continue;
            }

            return keyValue();
        }
    }

    /**
     * Moves the cursor to the last leaf element under the last page in the stack.
     */
    private void doLast() {
        var ref = stack.get(stack.size() - 1);

        while (!ref.isLeaf()) {
            long pageId;
            if (ref.node != null) {
                pageId = ref.node.inodes.get(ref.index).pageId;
            } else {
                pageId = ref.page.getBranchElement(ref.index).getPageId();
            }

            var pageNode = bucket.pageNode(pageId);
            ref = new ElemRef(pageNode.first(), pageNode.second());
            ref.index = ref.count() - 1;
            stack.add(ref);
        }
    }

    /**
     * Recursively performs a binary search against a given page/node until it finds a given key.
     *
     * @param key    Key to search
     * @param pageId Long page id.
     */
    private void search(final MemorySegment key, final long pageId) {
        var pageNode = bucket.pageNode(pageId);
        var page = pageNode.first();

        var ref = new ElemRef(page, pageNode.second());
        stack.add(ref);

        if (ref.isLeaf()) {
            nsearch(key);
            return;
        }

        var node = pageNode.second();
        if (node != null) {
            searchNode(key, node);
            return;
        }

        searchPage(key, page);
    }

    /**
     * Perform recursive search of tree for given key starting from current branch node
     *
     * @param key  Key to search
     * @param node Branch node to search inside
     */
    private void searchNode(final MemorySegment key, final Node node) {
        @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
        var iNodesKeyView = Lists.transform(node.inodes, iNodeKeyViewTransformer);
        var index = Collections.binarySearch(iNodesKeyView, key, MemorySegmentComparator.INSTANCE);

        //there is no exact match of the key
        if (index < 0) {
            //index of the first node which contains all keys which are bigger than current one
            index = -index - 1;

            if (index > 0) {
                index--;
            }
        }

        stack.get(stack.size() - 1).index = index;

        // Recursively search to the next page.
        search(key, node.inodes.get(index).pageId);
    }

    /**
     * Perform recursive search of tree for given key starting from current branch page
     *
     * @param key  Key to search
     * @param page Branch page to search inside
     */
    private void searchPage(final MemorySegment key, final BTreePage page) {
        @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
        var pagesKeyView = Lists.transform(page.getBranchElements(), branchPageKeyViewTransformer);
        var index = Collections.binarySearch(pagesKeyView, key, MemorySegmentComparator.INSTANCE);

        //there is no exact match of the key
        if (index < 0) {
            //index of the first node which contains all keys which are bigger than current one
            index = -index - 1;

            if (index > 0) {
                index--;
            }
        }

        stack.get(stack.size() - 1).index = index;

        // Recursively search to the next page.
        search(key, page.getBranchElement(index).getPageId());
    }

    /**
     * Searches the leaf node on the top of the stack for a key.
     *
     * @param key key to search.
     */
    private void nsearch(final MemorySegment key) {
        var ref = stack.get(stack.size() - 1);
        var node = ref.node;
        if (node != null) {
            @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
            var iNodesKeyView = Lists.transform(node.inodes, iNodeKeyViewTransformer);
            int index = Collections.binarySearch(iNodesKeyView, key, MemorySegmentComparator.INSTANCE);

            if (index < 0) {
                index = -index - 1;
            }
            ref.index = index;

            return;
        }

        @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
        var pagesKeyView = Lists.transform(ref.page.getLeafElements(), leafPageKeyViewTransformer);
        int index = Collections.binarySearch(pagesKeyView, key, MemorySegmentComparator.INSTANCE);
        if (index < 0) {
            index = -index - 1;
        }

        ref.index = index;
    }


    /**
     * @return key and value of the current leaf element or null if cursor reaches the end of leaf element.
     */
    private ObjectObjectIntTriple<MemorySegment, MemorySegment> keyValue() {
        var ref = stack.get(stack.size() - 1);
        if (ref.count() == 0 || ref.index >= ref.count()) {
            return null;
        }

        if (ref.node != null) {
            var inode = ref.node.inodes.get(ref.index);
            return new ObjectObjectIntTriple<>(inode.key, inode.value, inode.flags);
        }

        var elem = ref.page.getLeafElement(ref.index);
        return new ObjectObjectIntTriple<>(elem.getKey(), elem.getValue(), elem.getFlags());
    }


    /**
     * Represents a reference to an element on a given page/node.
     */
    private static final class ElemRef {
        final BTreePage page;
        Node node;

        int index;

        private ElemRef(BTreePage page, Node node) {
            this.page = page;
            this.node = node;
        }

        /**
         * @return whether the ref is pointing at a leaf page/node.
         */
        boolean isLeaf() {
            if (node != null) {
                return node.isLeaf();
            }

            return (page.getFlags() & Page.LEAF_PAGE_FLAG) != 0;
        }

        /**
         * @return number of inodes or page elements.
         */
        int count() {
            if (node != null) {
                return node.inodes.size();
            }

            return page.getCount();
        }
    }

    /**
     * If MemorySegment is mapped by mmap and current tx is writable tx then it could be invalidated during remmaping.
     * To prevent this MemorySegment is copied.
     *
     * @param memorySegment Segment to copy
     * @return New segment instance with the same data if needed.
     */
    private ByteBuffer unmap(MemorySegment memorySegment) {
        if (bucket.tx.writable && memorySegment.isNative()) {
            var data = new byte[(int) memorySegment.byteSize()];
            var segment = MemorySegment.ofArray(data);
            segment.copyFrom(memorySegment);

            return memorySegment.asByteBuffer();
        }

        return memorySegment.asByteBuffer();
    }
}
