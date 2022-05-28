package io.viktoriadb;

import jdk.incubator.foreign.MemorySegment;
import org.junit.Assert;
import org.junit.Test;

public class PageTest {
    @Test
    public void testPageAssignments() {
        var segment = MemorySegment.ofArray(new byte[4 * 1024]);
        var page = Page.createNewPage(segment, Page.PageType.BRANCH_PAGE);

        page.setPageId(12);
        page.setOverflow(23);

        Assert.assertTrue(page instanceof BTreePage);
        Assert.assertEquals(12, page.getPageId());
        Assert.assertEquals(23, page.getOverflow());
        Assert.assertEquals(Page.BRANCH_PAGE_FLAG, page.getFlags());
    }

    @Test
    public void testPageInitializationLeafPage() {
        var segment = MemorySegment.ofArray(new byte[4 * 1024]);
        var page = Page.createNewPage(segment, Page.PageType.LEAF_PAGE);

        Assert.assertTrue(page instanceof BTreePage);
        Assert.assertEquals(Page.LEAF_PAGE_FLAG, page.getFlags());
    }

    @Test
    public void testPageInitializationMetaPage() {
        var segment = MemorySegment.ofArray(new byte[4 * 1024]);
        var page = Page.createNewPage(segment, Page.PageType.META_PAGE);

        Assert.assertTrue(page instanceof Meta);
        Assert.assertEquals(Page.META_PAGE_FLAG, page.getFlags());
    }

    @Test
    public void testPageInitializationFreeListPage() {
        var segment = MemorySegment.ofArray(new byte[4 * 1024]);
        var page = Page.createNewPage(segment, Page.PageType.FREE_LIST_PAGE);
        Assert.assertEquals(Page.FREE_LIST_PAGE_FLAG, page.getFlags());
    }
}
