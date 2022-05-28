package io.viktoriadb;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import jdk.incubator.foreign.MemorySegment;
import org.junit.Assert;
import org.junit.Test;

public class FreeListTest {
    @Test
    public void testFree() {
        var page = createBucketPage();
        page.setPageId(12);

        var freeList = new FreeList();
        freeList.free(100, page);

        Assert.assertTrue(freeList.ids.isEmpty());
        Assert.assertEquals(1, freeList.pending.size());
        Assert.assertEquals(1, freeList.pending.get(100).size());
        Assert.assertEquals(12, freeList.pending.get(100).getLong(0));
    }

    @Test
    public void testFreeWithOverflow() {
        var freeList = new FreeList();
        var page = createBucketPage();

        page.setPageId(12);
        page.setOverflow(3);

        freeList.free(100, page);

        Assert.assertTrue(freeList.ids.isEmpty());
        Assert.assertEquals(1, freeList.pending.size());
        Assert.assertEquals(new LongArrayList(new long[]{12, 13, 14, 15}), freeList.pending.get(100));
    }

    @Test
    public void testRelease() {
        var freeList = new FreeList();

        var pageOne = createBucketPage();
        pageOne.setPageId(12);
        pageOne.setOverflow(1);

        var pageTwo = createBucketPage();
        pageTwo.setPageId(9);

        var pageThree = createBucketPage();
        pageThree.setPageId(39);

        freeList.free(100, pageOne);
        freeList.free(100, pageTwo);

        freeList.free(102, pageThree);

        freeList.release(100);
        freeList.release(101);

        Assert.assertEquals(new LongArrayList(new long[]{9, 12, 13}), freeList.ids);
        Assert.assertEquals(1, freeList.pending.size());
        Assert.assertEquals(new LongArrayList(new long[]{39}), freeList.pending.get(102));

        freeList.release(102);
        Assert.assertTrue(freeList.pending.isEmpty());
        Assert.assertEquals(new LongArrayList(new long[]{9, 12, 13, 39}), freeList.ids);
    }

    @Test
    public void testAllocate() {
        var freeList = new FreeList();
        freeList.ids = new LongArrayList(new long[]{3, 4, 5, 6, 7, 9, 12, 13, 18});
        for (int i = 0; i < freeList.ids.size(); i++) {
            freeList.cache.add(freeList.ids.getLong(i));
        }

        long pageId = freeList.allocate(3);
        Assert.assertEquals(3, pageId);

        pageId = freeList.allocate(1);
        Assert.assertEquals(6, pageId);

        pageId = freeList.allocate(3);
        Assert.assertEquals(0, pageId);

        pageId = freeList.allocate(2);
        Assert.assertEquals(12, pageId);

        pageId = freeList.allocate(1);
        Assert.assertEquals(7, pageId);

        pageId = freeList.allocate(0);
        Assert.assertEquals(0, pageId);

        pageId = freeList.allocate(0);
        Assert.assertEquals(0, pageId);

        Assert.assertEquals(new LongArrayList(new long[]{9, 18}), freeList.ids);

        pageId = freeList.allocate(1);
        Assert.assertEquals(9, pageId);

        pageId = freeList.allocate(1);
        Assert.assertEquals(18, pageId);

        pageId = freeList.allocate(1);
        Assert.assertEquals(0, pageId);

        Assert.assertTrue(freeList.ids.isEmpty());
        Assert.assertTrue(freeList.pending.isEmpty());
        Assert.assertTrue(freeList.cache.isEmpty());
    }

    @Test
    public void testReadWrite() {
        var freeList = new FreeList();
        freeList.ids = new LongArrayList(new long[]{12, 39});

        freeList.pending.put(100, new LongArrayList(new long[]{28, 11}));
        freeList.pending.put(101, new LongArrayList(new long[]{3}));


        var freePage = createFreeListPage();
        freeList.write(freePage);

        var freeList0 = new FreeList();
        freeList0.read(freePage);

        Assert.assertEquals(new LongArrayList(new long[]{3, 11, 12, 28, 39}), freeList0.ids);
        for (int i = 0; i < freeList0.ids.size(); i++) {
            final long pageId = freeList0.ids.getLong(i);
            Assert.assertTrue(freeList0.cache.contains(pageId));
        }
    }

    private static Page createBucketPage() {
        var memorySegment = MemorySegment.ofArray(new byte[4 * 1024]);
        return Page.createNewPage(memorySegment, Page.PageType.BRANCH_PAGE);
    }

    private static Page createFreeListPage() {
        var memorySegment = MemorySegment.ofArray(new byte[4 * 1024]);
        return Page.createNewPage(memorySegment, Page.PageType.FREE_LIST_PAGE);
    }
}
