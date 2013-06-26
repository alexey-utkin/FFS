package com.test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 *
 * Classical heap-on-array algorithm.
 *
 * Allocates cluster chains.
 * Bad   point: reverse index sequence in chain.
 * Good  point: O(count)
 */

public class FATFreeListClusterAllocator implements FATClusterAllocator {
    private final FATSystem fs;
    private int freeListHead;

    FATFreeListClusterAllocator(FATSystem _fs) {
        fs = _fs;
        freeListHead = -1;
    }


    /**
     * Initializes FAT System.
     */
    @Override
    public void initFAT() {
        // init FAT32
        for (int i = 0; i < fs.clusterCount - 1; ++i) {
            fs.putFatEntry(i, fs.CLUSTER_FREE | (i + 1));
        }
        fs.putFatEntry(fs.clusterCount - 1, fs.CLUSTER_FREE_EOC);
        freeListHead = 0;
    }

    /**
     * Allocates a cluster chain. Classical heap-on-array algorithm.
     * Bad   point: reverse index sequence in chain.
     * Good  point: O(count)
     *
     * @param tailCluster the index of the tail of the chain.
     *                  The [-1] value means that the chain should not be joined.
     *                  Any other value means that allocated chain will join to [tailCluster] tail
     * @param count     the number of cluster in the returned chain
     * @return the index of the first cluster in allocated chain.
     * @throws java.io.IOException if the chain could not be allocated
     */
    @Override
    public int allocateClusters(int tailCluster, int count) throws IOException {
        int headCluster = -1;
        int tailOffset = tailCluster;
        while (count > 0 && freeListHead >= 0) {
            int fatEntry = fs.getFatEntry(freeListHead);
            if ((fatEntry & fs.CLUSTER_STATUS) == fs.CLUSTER_FREE || fatEntry == fs.CLUSTER_FREE_EOC) {
                if (headCluster == -1)
                    headCluster = freeListHead;

                // mark as EOC
                --fs.freeClusterCount;
                fs.putFatEntry(freeListHead, fs.CLUSTER_EOC);
                if (tailOffset != -1) {
                    // mark as ALLOCATED with forward index
                    fs.putFatEntry(tailOffset, fs.CLUSTER_ALLOCATED | freeListHead);
                }
                tailOffset = freeListHead;
                freeListHead = (fatEntry == fs.CLUSTER_FREE_EOC)
                    ? -1
                    : (fatEntry & fs.CLUSTER_INDEX);
                --count;
                if (count == 0)
                    return headCluster;
            } else {
                fs.LogError("Wrong value in free list.");
                break;
            }
        }

        fs.LogError("[freeClusterCount] has wrong value.");
        fs.setDirtyStatus();

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
     * @param headCluster  the head of the chain, do nothing for [-1]
     * @param freeHead     if [true] the chain is freed together with head cluster
     *                     else head cluster is marked as [EOC]
     * @throws java.io.IOException
     */
    @Override
    public void freeClusters(int headCluster, boolean freeHead) throws IOException {
        if (headCluster < 0)
            return;

        //todo: improve: start release clusters from tail.
        if (!freeHead) {
            int fatEntry = fs.getFatEntry(headCluster);
            // CLUSTER_ALLOCATED only
            if ((fatEntry & fs.CLUSTER_STATUS) == fs.CLUSTER_ALLOCATED) {
                // mark as EOC
                fs.putFatEntry(headCluster, fs.CLUSTER_EOC);
                headCluster = fatEntry & fs.CLUSTER_INDEX;
            } else {
                fs.LogError("Cluster double free in tail.  Cluster#:" + headCluster
                        + " Value:" + fatEntry);
                fs.setDirtyStatus();
            }
        }
        while (true) {
            int fatEntry = fs.getFatEntry(headCluster);
            // CLUSTER_ALLOCATED or CLUSTER_EOC
            if ((fatEntry & fs.CLUSTER_ALLOCATED) == fs.CLUSTER_ALLOCATED) {
                // mark as DEALLOC
                if (freeListHead == -1) {
                    freeListHead = headCluster;
                    fs.putFatEntry(headCluster, fs.CLUSTER_FREE_EOC);
                } else {
                    fs.putFatEntry(headCluster, fs.CLUSTER_FREE | freeListHead);
                    freeListHead = headCluster;
                }
                ++fs.freeClusterCount;
                if ((fatEntry & fs.CLUSTER_EOC) == fs.CLUSTER_EOC)
                    break;
                headCluster = fatEntry & fs.CLUSTER_INDEX;
            } else {
                fs.LogError("Cluster double free. Cluster#:" + headCluster
                        + " Value:" + fatEntry);
                fs.setDirtyStatus();
            }
        }
    }

    /**
     * Restores state from storage
     */
    @Override
    public void initFromFile() {
         freeListHead = fs.getFatEntry(-1);
    }

    /**
     * Flush critical data to host system.
     */
    @Override
    public void force() {
        fs.putFatEntry(-1, freeListHead);
    }
}
