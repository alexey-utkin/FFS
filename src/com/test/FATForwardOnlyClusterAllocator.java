package com.test;

import java.io.IOException;

/**
 * Forward Only Allocator. debug/maintenance
 *
 * Allocates cluster chains. Free haunter algorithm.
 * Bad   point: O(fs.clusterCount)
 * Good  point: the best monotonic index sequence in chain.
 *              visible watermark, parameters for save.
 * Applicable for system with fs.clusterCount ~ count. Perfect for maintenance,
 * when free list is damaged.
 *
 * Improvements: for write-mostly system - support the hint [startOffset]
 * as last allocated index.
 */
class FATForwardOnlyClusterAllocator implements FATClusterAllocator {
    // Diagnostic
    final static int CLUSTER_UNUSED    = 0x0BADBEEF;
    final static int CLUSTER_DEALLOC   = 0x0CCCCCCC;

    private final FATSystem fs;

    FATForwardOnlyClusterAllocator(FATSystem _fs) {
        fs = _fs;
    }


    /**
     * Initializes FAT System.
     */
    @Override
    public void initFAT() throws IOException {
        // init FAT32
        for (int i = 0; i < fs.clusterCount; ++i) {
            fs.putFatEntry(i, CLUSTER_UNUSED);
        }
    }

    /**
     * Allocates a cluster chain. Free haunter algorithm.
     * Bad   point: O(fs.clusterCount)
     * Good  point: the best monotonic index sequence in chain.
     *              visible watermarks
     * Applicable for system with fs.clusterCount ~ count.
     *
     * @param tailCluster the index of the tail of the chain.
     *                  The -1 value means that the chain should not be joined.
     *                  Any other value means that allocated chain will join to [tailCluster] tail
     * @param count     the number of cluster in the returned chain
     * @return the index of the first cluster in allocated chain.
     * @throws java.io.IOException if the chain could not be allocated
     */
    @Override
    public int allocateClusters(int tailCluster, int count) throws IOException {
        int currentOffset = (tailCluster == -1)
                ? 0
                : tailCluster + 1;
        int headCluster = -1;
        int tailOffset = tailCluster;
        int endOfLoop = currentOffset; // loop marker
        while (true) {
            if (currentOffset >= fs.clusterCount) {
                currentOffset = 0;
                if (currentOffset == endOfLoop)
                    break;
            }

            int fatEntry = fs.getFatEntry(currentOffset);
            if ((fatEntry & CLUSTER_STATUS) == CLUSTER_FREE) {
                if (headCluster == -1)
                    headCluster = currentOffset;

                // "God, save EOC on power down!"
                // mark as EOC
                --fs.freeClusterCount;
                fs.putFatEntry(currentOffset, CLUSTER_EOC);
                if (tailOffset != -1) {
                    // mark as ALLOCATED with forward index
                    fs.putFatEntry(tailOffset, CLUSTER_ALLOCATED | currentOffset);
                }
                tailOffset = currentOffset;
                --count;
                if (count == 0)
                    return headCluster;
            }
            ++currentOffset;
            if (currentOffset == endOfLoop)
                break;
        }

        fs.setDirtyState("[freeClusterCount] has wrong value.",  false);

        // rollback allocation.
        if (tailCluster == -1)
            freeClusters(headCluster, true);
        else
            freeClusters(tailCluster, false);

        throw new IOException("Disk full.");
    }

    /**
     * Frees chain that start from [headCluster] cluster.
     *
     * @param headCluster   the head of the chain, do nothing for [-1]
     * @param freeHead     if [true] the chain is freed together with head cluster
     *                     else head cluster is marked as [EOC]
     * @throws java.io.IOException
     */
    @Override
    public void freeClusters(int headCluster, boolean freeHead) throws IOException {
        if (headCluster < 0 )
            return;
        if (!freeHead) {
            int fatEntry = fs.getFatEntry(headCluster);
            // CLUSTER_ALLOCATED only
            if ((fatEntry & CLUSTER_STATUS) == CLUSTER_ALLOCATED) {
                // mark as EOC
                fs.putFatEntry(headCluster, CLUSTER_EOC);
                headCluster = fatEntry & CLUSTER_INDEX;
            } else {
                fs.setDirtyState("Cluster double free in tail.  Cluster#:" + headCluster
                        + " Value:" + fatEntry, true);
            }
        }
        while (true) {
            int fatEntry = fs.getFatEntry(headCluster);
            // CLUSTER_ALLOCATED or CLUSTER_EOC
            if ((fatEntry & CLUSTER_ALLOCATED) == CLUSTER_ALLOCATED) {
                // mark as DEALLOC
                fs.putFatEntry(headCluster, CLUSTER_DEALLOC);
                ++fs.freeClusterCount;
                if ((fatEntry & CLUSTER_EOC) == CLUSTER_EOC)
                    break;
                headCluster = fatEntry & CLUSTER_INDEX;
            } else {
                fs.setDirtyState("Cluster double free. Cluster#:" + headCluster
                        + " Value:" + fatEntry, true);
            }
        }
    }

    /**
     * Restores allocator state from storage
     */
    @Override
    public void initFromFile() {
        //nothing to do
    }

    /**
     * Flush critical data to host system.
     */
    @Override
    public void force() {
        //nothing to do
    }
}
