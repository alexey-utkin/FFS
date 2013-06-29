/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 *
 * @author uta
 */
public class FATSystemTest {
    private static FileSystem fs = FileSystems.getDefault();

    private static final int[] allocatorTypes = new int[] {
            FATSystem.ALLOCATOR_CLASSIC_HEAP,
            FATSystem.ALLOCATOR_FAST_FORWARD};
    private static final String[] allocationTypeNames = new String[] {
            "ClassicHeap",
            "FastForward"};

    private static void log(String message) {
        System.out.print(message);
    }
    private static void logLN(String message) {
        System.out.println(message);
    }
    static public void startUp(Path hostPath) throws IOException {
        Files.deleteIfExists(hostPath);
    }
    static public void tearDown(Path hostPath) throws IOException {
        //Files.deleteIfExists(hostPath);
    }
    static public void logStart(Path path, int clusterSize, int clusterCount,
                                int allocatorType)
    {
        log(    path + "/" + allocationTypeNames[allocatorType]
                + " cs=" + clusterSize
                + " cc=" + clusterCount
                + ":");
    }
    static public void logOk() {
        logLN("Ok");
    }

    @Rule
    public TestName name = new TestName();

    public Path getPath() {
        return fs.getPath(name.getMethodName() + ".fs");
    }


    public FATSystemTest() throws IOException {
        testFileCreate();
    }


    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of File creation.
     */
    static public void testFileCreate(Path path, int clusterSize, int clusterCount,
                                  int allocatorType) throws IOException {
        startUp(path);
        try (FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount, allocatorType)) {

        }
        tearDown(path);
    }

    @Test
    public void testFileCreate() throws IOException {
        final int[] clusterCounts = new int[] {
                32
        };
        int clusterSize = 4096;
        for (int allocatorType : allocatorTypes) {
            for (int clusterCount : clusterCounts) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testFileCreate(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }


    /**
     * Test of FS creation.
     */
    static public void testCreate(Path path, int clusterSize, int clusterCount,
                                  int allocatorType) throws IOException {
        startUp(path);
        try (FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            if (ffs.getSize() != clusterSize*clusterCount)
                throw new Error("Wrong storage size!");
        }
        tearDown(path);
    }
    
    @Test
    public void testCreate() throws IOException {
        final int[] clusterCounts = new int[] {
                16, 32, 2
                //(int)(0x800000000L/FolderEntry.RECORD_SIZE)
        };
        int clusterSize = FolderEntry.RECORD_SIZE;
        for (int allocatorType : allocatorTypes) {
            for (int clusterCount : clusterCounts) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testCreate(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }

    /**
     * Test of FS Critical Fat Allocation.
     */
    static public void testCriticalFatAllocation(Path path, int clusterSize, int clusterCount,
                                                 int allocatorType) throws IOException {
        startUp(path);
        try (FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            // 0 size
            try {
                ffs.allocateClusters(-1, 0);
                throw new Error("Zero allocation available!");
            } catch (IOException fe) {
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
            } catch (IOException fe) {
                //OK
            }

            // full size-1 - re-alloc!
            int firstCluster = ffs.allocateClusters(zeroCluster, clusterCount-1);
            if (firstCluster != 1)
                throw new Error("Bad re-allocation for root folder!");

            ffs.freeClusters(zeroCluster, true);

            // full size fail success
            ffs.allocateClusters(-1, clusterCount);
        }
        tearDown(path);
    }
    
    @Test
    public void testCriticalFatAllocation() throws IOException {
        int[] clusterCounts = new int[] {
                16, 32,
                //(int)(0x80000000L/FolderEntry.RECORD_SIZE)
        };
        int clusterSize = FolderEntry.RECORD_SIZE;
        for (int allocatorType : allocatorTypes) {
            for (int clusterCount : clusterCounts) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testCriticalFatAllocation(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }

    /**
     * Test of FS Concurrent Fragmentation.
     */
    static public void testConcurrentFragmentation(Path path, int clusterSize, int clusterCount,
                                                   int allocatorType) throws IOException {
        startUp(path);
        try (FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount, allocatorType)) {
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
            if (clusterCount <= totalCount)
                throw new Error("Bad test parameters");
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
                throw new IOException("Concurrent access problem: Lost clusters.");
        }
        tearDown(path);
    }
    
    @Test
    public void testConcurrentFragmentation() throws IOException {
        // size must be > 30
        int[] clusterCounts = new int[] {
                31, 1024, 4096
        };
        int clusterSize = FolderEntry.RECORD_SIZE;
        for (int allocatorType : allocatorTypes) {
            for (int clusterCount : clusterCounts) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testConcurrentFragmentation(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }

    /**
     * Test of FS Concurrent Safe Close.
     */
    static public void testConcurrentSafeClose(Path path, int clusterSize,
                                               int clusterCount, int allocatorType) throws IOException {
        startUp(path);

        final FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount, allocatorType);

        /* random size allocation */
        final int[] fragmentLengths = new int[] {1, 2, 4, 5};
        if (clusterCount < 12)
            throw new Error("Bad test parameters");

        /* stress allocation */
        Thread[] actions = new Thread[fragmentLengths.length];
        final Throwable[] errors = new Throwable[fragmentLengths.length];
        for (int i = 0; i < fragmentLengths.length; ++i) {
            final int actionI = i;
            actions[i] = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        int start = ffs.allocateClusters(-1, fragmentLengths[actionI]);
                        Thread.sleep(0xF); // switch thread
                        ffs.freeClusters(start, true);
                    } catch (Throwable e) {
                        errors[actionI] = e;
                    }
                }
            });
            actions[i].start();
        }

        //Async close is not a reason for [dirty storage]!
        try {
            Thread.sleep(0x5); // switch thread
        } catch (InterruptedException e) {
            //OK
        }
        ffs.close();

        // dump status without any throw
        for (int i = 0; i < fragmentLengths.length; ++i) {
            try {
                actions[i].join();
            } catch (InterruptedException e) {
                System.err.println("System panic: synchronization!");
                if (actions[i].isAlive())
                    --i;
            }
            if (errors[i] != null
             && "The storage was closed.".equals(errors[i].getMessage()))
                throw new Error("Wrong incident:" + errors[i].getMessage());
        }

        //should be:
        // --accessible for open
        // --clean (not dirty)
        FATSystem.open(path, true).close();

        tearDown(path);
    }
    
    @Test
    public void testConcurrentSafeClose() throws IOException {
        int clusterCount = 16;// > 12
        int clusterSize = FolderEntry.RECORD_SIZE;
        for (int allocatorType : allocatorTypes) {
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testConcurrentSafeClose(getPath(),
                    clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

    /**
     * Test of FS Concurrent Safe Close.
     */
    static public void testOpen(Path path, int clusterSize,
                                int clusterCount, int allocatorType) throws IOException {
        startUp(path);

        final FATSystem ffs1  = FATSystem.create(path, clusterSize, clusterCount, allocatorType);
        int first = ffs1.allocateClusters(-1, clusterCount/2);
        ffs1.close();

        final FATSystem ffs2  = FATSystem.open(path, true);

        if (ffs2.getSize() != clusterCount*FolderEntry.RECORD_SIZE)
            throw new Error("Wrong storage size!");

        if (ffs2.getFreeSize() != clusterCount*FolderEntry.RECORD_SIZE/2)
            throw new Error("Wrong storage free size!");

        //test join, test sequential allocation on free disk
        int second = ffs2.allocateClusters(first + clusterCount/2 - 1, clusterCount/2);
        if (ffs2.getFreeSize() != 0)
            throw new Error("Wrong storage state: FAT alloc!");

        ffs2.freeClusters(first, true);
        if (ffs2.getFreeSize() != clusterCount*FolderEntry.RECORD_SIZE)
            throw new Error("Wrong storage state: FAT join!");

        ffs2.close();

        tearDown(path);
    }
    
    @Test
    public void testOpen() throws IOException {
        int clusterCount = 16;// > 12
        int clusterSize = FolderEntry.RECORD_SIZE;
        for (int allocatorType : allocatorTypes) {
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testOpen(getPath(), clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }
}