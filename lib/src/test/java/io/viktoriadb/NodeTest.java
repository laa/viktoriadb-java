package io.viktoriadb;

import io.viktoriadb.util.MemorySegmentComparator;
import jdk.incubator.foreign.MemorySegment;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class NodeTest {
    @Test
    public void testPut() {
        var node = new Node();
        var tx = new Tx();

        tx.meta = createMetaPage();
        tx.meta.setMaxPageId(1);

        node.bucket = new Bucket(tx);
        node.put(bytes("baz"), bytes("baz"), bytes("2"), 0, 0);
        node.put(bytes("foo"), bytes("foo"), bytes("0"), 0, 0);
        node.put(bytes("bar"), bytes("bar"), bytes("1"), 0, 0);
        node.put(bytes("foo"), bytes("foo"), bytes("3"), 0, 1);

        Assert.assertEquals(3, node.inodes.size());

        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("bar"), node.inodes.get(0).key));
        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("1"), node.inodes.get(0).value));

        Assert.assertEquals(0, MemorySegmentComparator.INSTANCE.compare(bytes("baz"),
                node.inodes.get(1).key));
        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("2"), node.inodes.get(1).value));

        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("foo"), node.inodes.get(2).key));
        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("3"), node.inodes.get(2).value));

        Assert.assertEquals(1, node.inodes.get(2).flags);
    }

    @Test
    public void testWrite() {
        var node = new Node();
        var tx = new Tx();

        tx.meta = createMetaPage();
        tx.meta.setMaxPageId(1);

        node.bucket = new Bucket(tx);
        node.isLeaf = true;

        node.put(bytes("susy"), bytes("susy"), bytes("que"), 0, 1);
        node.put(bytes("ricki"), bytes("ricki"), bytes("lake"), 0, 2);
        node.put(bytes("john"), bytes("john"), bytes("johnson"), 0, 3);

        var page = createBTreePage();
        node.write(page);

        var node0 = new Node();
        node0.read(page);

        Assert.assertEquals(3, node0.inodes.size());

        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("john"), node0.inodes.get(0).key));
        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("johnson"), node0.inodes.get(0).value));
        Assert.assertEquals(3, node0.inodes.get(0).flags);

        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("ricki"), node0.inodes.get(1).key));
        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("lake"), node0.inodes.get(1).value));
        Assert.assertEquals(2, node0.inodes.get(1).flags);

        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("susy"), node0.inodes.get(2).key));
        Assert.assertEquals(0,
                MemorySegmentComparator.INSTANCE.compare(bytes("que"), node0.inodes.get(2).value));
        Assert.assertEquals(1, node0.inodes.get(2).flags);
    }

    @Test
    public void testSplit() {
        var node = new Node();
        var tx = new Tx();

        tx.meta = createMetaPage();
        tx.meta.setMaxPageId(1);

        node.bucket = new Bucket(tx);
        node.isLeaf = true;

        node.put(bytes("00000001"), bytes("00000001"), bytes("0123456701234567"), 0, 0);
        node.put(bytes("00000002"), bytes("00000002"), bytes("0123456701234567"), 0, 0);
        node.put(bytes("00000003"), bytes("00000003"), bytes("0123456701234567"), 0, 0);
        node.put(bytes("00000004"), bytes("00000004"), bytes("0123456701234567"), 0, 0);
        node.put(bytes("00000005"), bytes("00000005"), bytes("0123456701234567"), 0, 0);

        // Split between 2 & 3.
        node.split(200);

        var parent = node.parent;
        Assert.assertEquals(2, parent.children.size());

        Assert.assertEquals(2, parent.children.get(0).inodes.size());
        Assert.assertEquals(3, parent.children.get(1).inodes.size());
    }

    @Test
    public void splitMinKeys() {
        var node = new Node();
        var tx = new Tx();

        tx.meta = createMetaPage();
        tx.meta.setMaxPageId(1);

        node.bucket = new Bucket(tx);

        node.put(bytes("00000001"), bytes("00000001"), bytes("0123456701234567"), 0, 0);
        node.put(bytes("00000002"), bytes("00000002"), bytes("0123456701234567"), 0, 0);

        node.split(20);

        Assert.assertNull(node.parent);
    }

    @Test
    public void testSplitSinglePage() {
        var node = new Node();
        var tx = new Tx();

        tx.meta = createMetaPage();
        tx.meta.setMaxPageId(1);

        node.bucket = new Bucket(tx);
        node.isLeaf = true;

        node.put(bytes("00000001"), bytes("00000001"), bytes("0123456701234567"), 0, 0);
        node.put(bytes("00000002"), bytes("00000002"), bytes("0123456701234567"), 0, 0);
        node.put(bytes("00000003"), bytes("00000003"), bytes("0123456701234567"), 0, 0);
        node.put(bytes("00000004"), bytes("00000004"), bytes("0123456701234567"), 0, 0);
        node.put(bytes("00000005"), bytes("00000005"), bytes("0123456701234567"), 0, 0);

        node.split(4096);

        Assert.assertNull(node.parent);
    }

    private static Meta createMetaPage() {
        var memorySegment = MemorySegment.ofArray(new byte[1024]);
        return (Meta) Page.createNewPage(memorySegment, Page.PageType.META_PAGE);
    }

    private static BTreePage createBTreePage() {
        var memorySegment = MemorySegment.ofArray(new byte[1024]);
        return (BTreePage) Page.createNewPage(memorySegment, Page.PageType.LEAF_PAGE);
    }

    private static MemorySegment bytes(String string) {
        var bytes = string.getBytes(StandardCharsets.UTF_8);
        return MemorySegment.ofArray(bytes);
    }
}
