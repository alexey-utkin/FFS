/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author uta
 */
public class FATSystemTest {
    static FileSystem fs = FileSystems.getDefault();    
    private static void Log(String message) {
        System.out.print(message);
    }
    private static void LogLN(String message) {
        System.out.println(message);
    }
    static public void startUp(Path hostPath) throws IOException {
        Files.deleteIfExists(hostPath);
    }
    static public void tearDown(Path hostPath) throws IOException {
        Files.deleteIfExists(hostPath);
    }
    
    public FATSystemTest() {
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
     * Test of FS creation.
     */
    static public void testCreate(Path path, int clusterSize, int clusterCount) throws IOException {
        startUp(path);
        try (FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount)) {
            if (ffs.getSize() != clusterSize*clusterCount)
                throw new Error("Wrong storage size!");
        }
        tearDown(path);
    }
    
    @Test
    public void testCreate() throws IOException {
        int[] clusterCounts = new int[] {
                16, 32, 2
                //(int)(0x800000000L/FolderEntry.RECORD_SIZE)
        };
        for (int clusterCount : clusterCounts) {
            Log("testCreate FS Size " + clusterCount + ":");
            testCreate(fs.getPath("testCreate.fs"),
                FolderEntry.RECORD_SIZE,
                clusterCount);
            LogLN("OK");
        }
    }

    /**
     * Test of FS Critical Fat Allocation.
     */
    static public void testCriticalFatAllocation(Path path, int clusterSize, int clusterCount) throws IOException {
        startUp(path);
        try (FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount)) {
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
        for (int clusterCount : clusterCounts) {
            Log("testCriticalFatAllocation FS Size " + clusterCount + ":");
            testCriticalFatAllocation(fs.getPath("testCriticalFatAllocation.fs"),
                FolderEntry.RECORD_SIZE,
                clusterCount);
            LogLN("OK");
        }
    }

    /**
     * Test of FS Concurrent Fragmentation.
     */
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
        for (int clusterCount : clusterCounts) {
            Log("testConcurrentFragmentation FS Size " + clusterCount + ":");
            testConcurrentFragmentation(fs.getPath("testConcurrentFragmentation.fs"),
                    FolderEntry.RECORD_SIZE,
                    clusterCount);

            LogLN("OK");
        }
    }

    /**
     * Test of FS Concurrent Safe Close.
     */
    static public void testConcurrentSafeClose(Path path, int clusterSize, int clusterCount) throws IOException {
        startUp(path);

        final FATSystem ffs  = FATSystem.create(path, clusterSize, clusterCount);

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
        FATSystem.open(path).close();

        tearDown(path);
    }
    
    @Test
    public void testConcurrentSafeClose() throws IOException {
        int clusterCount = 16;// > 12
        Log("testConcurrentSafeClose FS Size " + clusterCount + ":");
        testConcurrentSafeClose(fs.getPath("testConcurrentFragmentation.fs"),
                FolderEntry.RECORD_SIZE,
                clusterCount);
        LogLN("OK");
    }

    /**
     * Test of FS Concurrent Safe Close.
     */
    static public void testOpen(Path path) throws IOException {
        startUp(path);
        int clusterCount = 4;

        final FATSystem ffs1  = FATSystem.create(path, FolderEntry.RECORD_SIZE, clusterCount);
        int first = ffs1.allocateClusters(-1, clusterCount/2);
        ffs1.close();

        final FATSystem ffs2  = FATSystem.open(path);

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
        Log("testOpen:");        
        testOpen(fs.getPath("testOpen.fs"));
        LogLN("OK");        
    }
}