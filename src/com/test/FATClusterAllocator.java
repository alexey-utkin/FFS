package com.test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 */
interface FATClusterAllocator {
    /**
     * Initializes FAT System.
     */
    void initFAT();

    /**
     * Allocates a cluster chain.
     *
     * @param tailCluster the index of the tail of the chain.
     *        The [-1] value means that the chain should not be joined.
     *        Any other value means that allocated chain will join to [tailCluster] tail
     * @param count the number of cluster in the returned chain
     * @return the index of the first cluster in allocated chain.
     * @throws java.io.IOException if the chain could not be allocated
     */
    int allocateClusters(int tailCluster, int count) throws IOException;

    /**
     * Frees chain that start from [headOffset] cluster.
     *
     * @param headOffset the head of the chain, do nothing for [-1]
     * @param freeHead if [true] the chain is freed together with head cluster
     *                 else head cluster is marked as [EOC]
     * @throws IOException
     */
    void freeClusters(int headOffset, boolean freeHead) throws IOException;

    /**
     * Restores state from storage
     */
    void initFromFile();

    /**
     * Flush critical data to host system.
     */
    void force();
}
