package com.test;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 */

public class FATSystemTests {
    //TEST
    static public void startUp(Path hostPath) throws IOException {
        Files.deleteIfExists(hostPath);
    }

    //@Test
    static public void tearDown(Path hostPath) throws IOException {
        Files.deleteIfExists(hostPath);
    }

    //@Test
    static public void testConcurrentFragmentation(Path path, int clusterSize, int clusterCount) throws IOException {
        startUp(path);
        try (FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount)) {
            final int circleCount = 15;

            /* random size allocation */
            final int[] fragmentLengths = new int[] {
                    1,
                    2,
                    clusterCount/2,
                    4,
                    5,
                    0/*rest*/};
            /* check the rest */
            int totalCount = 0;
            for (int m : fragmentLengths)
                totalCount += m;
            if (clusterCount - totalCount <= 0)
                new Error("Bad test parameters");
            fragmentLengths[fragmentLengths.length - 1] = clusterCount - totalCount;

            /* stress allocation */
            Thread[] actions = new Thread[fragmentLengths.length];
            final Throwable[] errors = new Throwable[fragmentLengths.length];
            final java.util.Random random = new java.util.Random();
            for (int i = 0; i < fragmentLengths.length; ++i) {
                final int actionI = i;
                actions[i] = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            Thread.sleep(random.nextInt() & 0x0FF); // random start
                            for (int k = 0; k < circleCount; ++k) {
                                int start = ffs.allocateClusters(-1, fragmentLengths[actionI]);
                                Thread.sleep(random.nextInt() & 0x0FF); // switch thread
                                if (k + 1 != circleCount)
                                    ffs.freeClusters(start, true);
                            }
                        } catch (Throwable e) {
                            errors[actionI] = e;
                        }
                    }
                });
                actions[i].start();
            }

            for (int i = 0; i < fragmentLengths.length; ++i) {
                try {
                    actions[i].join();
                } catch (InterruptedException e) {
                    System.err.println("System panic: synchronization!");
                    if (actions[i].isAlive())
                        --i;
                }
                if (errors[i] != null)
                    throw new IOException("Concurrent access problem:" + errors[i].getMessage(), errors[i]);
            }
            if (ffs.getFreeSize() != 0)
                throw new FileSystemException("Concurrent access problem: Lost clusters.");
        } finally {
            //tearDown(path);
        }
    }

    //@Test
    static public void testCriticalFatAllocation(Path path, int clusterSize, int clusterCount) throws IOException {
        startUp(path);
        try (FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount)) {
            // 0 size
            try {
                ffs.allocateClusters(-1, 0);
                throw new Error("Zero allocation available!");
            } catch (FileSystemException fe) {
                //OK
            }

            // 1 size
            int zeroCluster = ffs.allocateClusters(-1, 1);
            if (zeroCluster != 0)
                throw new Error("Bad allocation for root folder!");

            // full size fail
            try {
                ffs.allocateClusters(-1, clusterCount);
                throw new Error("Double use available!");
            } catch (FileSystemException fe) {
                //OK
            }

            // full size-1 - re-alloc!
            int firstCluster = ffs.allocateClusters(zeroCluster, clusterCount-1);
            if (firstCluster != 1)
                throw new Error("Bad re-allocation for root folder!");

            ffs.freeClusters(zeroCluster, true);

            // full size fail success
            ffs.allocateClusters(-1, clusterCount);
        } finally {
            tearDown(path);
        }
    }

    //@Test
    static public void testCreate(Path path, int clusterSize, int clusterCount) throws IOException {
        startUp(path);
        try (FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount)) {
        } finally {
            tearDown(path);
        }
    }
}
