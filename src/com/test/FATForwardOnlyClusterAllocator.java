package com.test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 *
 * Forward Only Debug Allocator
 */
class FATForwardOnlyClusterAllocator implements FATClusterAllocator {
    private final FATSystem fs;

    FATForwardOnlyClusterAllocator(FATSystem _fs) {
        fs = _fs;
    }


    /**
     * Initializes FAT System.
     */
    @Override
    public void initFAT() {
        // init FAT32
        for (int i = 0; i < fs.clusterCount; ++i) {
            fs.fatZone.putInt(fs.CLUSTER_UNUSED);
        }
    }

    /**
     * Allocates a cluster chain.
     *
     * @param findAfter the index of the tail of the chain.
     *                  The -1 value means that the chain should not be joined.
     *                  Any other value means that allocated cain will join to [findAfter] tail
     * @param count     the number of cluster in the returned chain
     * @return the index of the first cluster in allocated chain.
     * @throws java.io.IOException if the chain could not be allocated
     */
    @Override
    public int allocateClusters(int findAfter, int count) throws IOException {
        int currentOffset = (findAfter == -1)
                ? 0
                : findAfter + 1;
        int headOffset = -1;
        int tailOffset = findAfter;
        int endOfLoop = currentOffset; // loop marker
        while (true) {
            if (currentOffset >= fs.clusterCount) {
                currentOffset = 0;
                if (currentOffset == endOfLoop)
                    break;
            }

            int fatEntry = fs.fatZone.getInt(currentOffset*fs.FAT_E_SIZE);
            if ((fatEntry & fs.CLUSTER_STATUS) == fs.CLUSTER_FREE) {
                if (headOffset == -1)
                    headOffset = currentOffset;

                // mark as EOC
                --fs.freeClusterCount;
                fs.fatZone.putInt(currentOffset*fs.FAT_E_SIZE, fs.CLUSTER_EOC);
                if (tailOffset != -1) {
                    // mark as ALLOCATED with forward index
                    fs.fatZone.putInt(tailOffset*fs.FAT_E_SIZE, fs.CLUSTER_ALLOCATED | currentOffset);
                }
                tailOffset = currentOffset;
                --count;
                if (count == 0)
                    return headOffset;
            }
            ++currentOffset;
            if (currentOffset == endOfLoop)
                break;
        }

        fs.LogError("[freeClusterCount] has wrong value.");
        fs.setDirtyStatus();

        // rollback allocation.
        if (findAfter == -1)
            freeClusters(headOffset, true);
        else
            freeClusters(findAfter, false);

        throw new IOException("Disk full.");
    }

    /**
     * Frees chain that start from [headOffset] cluster.
     *
     * @param headOffset   the head of the chain
     * @param freeHead     if [true] the chain is freed together with head cluster
     *                     else head cluster is marked as [EOC]
     * @throws java.io.IOException
     */
    @Override
    public void freeClusters(int headOffset, boolean freeHead) throws IOException {
        if (!freeHead) {
            int value = fs.fatZone.getInt(headOffset*fs.FAT_E_SIZE);
            // CLUSTER_ALLOCATED only
            if ((value & fs.CLUSTER_STATUS) == fs.CLUSTER_ALLOCATED) {
                // mark as EOC
                fs.fatZone.putInt(headOffset*fs.FAT_E_SIZE, fs.CLUSTER_EOC);
                headOffset = value & fs.CLUSTER_INDEX;
            } else {
                fs.LogError("Cluster double free in tail.  Cluster#:" + headOffset
                        + " Value:" + value);
                fs.setDirtyStatus();
            }
        }
        while (true) {
            int value = fs.fatZone.getInt(headOffset*fs.FAT_E_SIZE);
            // CLUSTER_ALLOCATED or CLUSTER_EOC
            if ((value & fs.CLUSTER_ALLOCATED) == fs.CLUSTER_ALLOCATED) {
                // mark as DEALLOC
                fs.fatZone.putInt(headOffset*fs.FAT_E_SIZE, fs.CLUSTER_DEALLOC);
                ++fs.freeClusterCount;
                if ((value & fs.CLUSTER_EOC) == fs.CLUSTER_EOC)
                    break;
                headOffset = value & fs.CLUSTER_INDEX;
            } else {
                fs.LogError("Cluster double free. Cluster#:" + headOffset
                        + " Value:" + value);
                fs.setDirtyStatus();
            }
        }
    }
}
