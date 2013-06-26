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
     * @param findAfter the index of the tail of the chain.
     *        The -1 value means that the chain should not be joined.
     *        Any other value means that allocated cain will join to [findAfter] tail
     * @param count the number of cluster in the returned chain
     * @return the index of the first cluster in allocated chain.
     * @throws java.io.IOException if the chain could not be allocated
     */
    int allocateClusters(int findAfter, int count) throws IOException;

    /**
     * Frees chain that start from [headOffset] cluster.
     *
     * @param headOffset the head of the chain
     * @param freeHead if [true] the chain is freed together with head cluster
     *                 else head cluster is marked as [EOC]
     * @throws IOException
     */
     void freeClusters(int headOffset, boolean freeHead) throws IOException;
}
