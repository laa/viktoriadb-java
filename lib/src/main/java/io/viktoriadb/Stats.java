package io.viktoriadb;

import java.util.concurrent.atomic.LongAdder;

/**
 * Stats represents statistics about the database.
 */
public final class Stats {
    // Freelist stats

    /**
     * Total number of free pages on the freelist
     */
    public int freePageN;

    /**
     * Total number of pending pages on the freelist
     */
    public int pendingPageN;

    /**
     * Total bytes allocated in free pages
     */
    public long freeAlloc;

    /**
     * Total bytes used by the FreeList
     */
    public long freeListInUse;

    // Transaction stats
    /**
     * Total number of started read transactions
     */
    public LongAdder txN = new LongAdder();

    /**
     * Number of currently open read transactions
     */
    public LongAdder openTxN = new LongAdder();

    /**
     * Global, ongoing stats.
     */
    public Tx.TxStats txStats = new Tx.TxStats();
}
