package com.test.API;

/**
 * Stress test fo File System.
 *
 * Hot Shutdown Test
 */

import com.test.FATBaseTest;
import com.test.FATFile;
import com.test.FATFileSystem;
import com.test.FATFolder;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FATFileSystemStress extends FATBaseTest {
    //
    // Test of FS shutdown test.
    //
    static public void testShutdown(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException {
        startUp(path);

        final FATFileSystem ffs1  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType);
        final FATFolder root1 = ffs1.getRoot();

        ExecutorService executor = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 100; i++) {
            Runnable worker = new Runnable() {
                @Override public void run() {
                    try {
                        root1.createFolder("Test1")
                                .createFolder("Test1_2")
                                .createFolder("Test1_3");

                        root1.deleteChildren();
                        if (root1.findFile("Test1") != null)
                            logLN("Concurrent add call.");

                        root1.createFolder("Test2");
                        root1.createFolder("Test3");
                        //root1.pack();
                    } catch (IOException ex) {
                        logLN(ex.toString());
                    }
                }
            };
            executor.execute(worker);
        }

        try {
            Thread.sleep(10);
            ffs1.waitForShutdown();
        } catch (InterruptedException ex) {
            //ok
        }
        ffs1.close();

        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
                //ok
            }
        }

        System.gc();

        final FATFileSystem ffs2  = FATFileSystem.open(path);
        FATFolder root2 = ffs2.getRoot();
        ffs2.close();

        tearDown(path);
    }
    
    @Test
    public void testShutdown() throws IOException {
        for (int allocatorType : allocatorTypes) {
            //allocatorType =   FATSystem.ALLOCATOR_FAST_FORWARD;
            int clusterSize = FATFile.RECORD_SIZE; //fixed!
            int clusterCount = 400; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testShutdown(getPath(), clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

    //
    //  Test of cross child move.
    //
    static public void testStressCrossChildMove(Path path, int clusterSize, int clusterCount,
                                          int allocatorType) throws IOException {
        startUp(path);

        String dump1;
        try (final FATFileSystem ffs = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            final FATFolder root1 = ffs.getRoot();
            ExecutorService executor = Executors.newFixedThreadPool(100);
            for (int i = 0; i < 100; i++) {
                final int[] ii = new int[] { i };
                Runnable worker = new Runnable() {
                    @Override public void run() {
                        try {
                            //1->1_1->1_1_1
                            final FATFolder f1 = root1.createFolder("" + ii[0] + "1");
                            final FATFolder f1_1 = f1.createFolder("1_1");
                            final FATFolder f1_1_1 = f1_1.createFolder("1_1_1");
                            final FATFolder f2 = root1.createFolder("" + ii[0] + "2");

                            final Object start = new Object();
                            final IOException problem[] = new IOException[]{null};
                            Thread mover = new Thread(new Runnable() {
                                @Override public void run() {
                                    try {
                                        synchronized (start) {
                                            start.notify();
                                        }
                                        try {
                                            Thread.sleep(10);
                                        } catch (InterruptedException e) {
                                            //ok
                                        }
                                        f2.asFile().moveTo(f1_1_1);
                                    } catch (IOException e) {
                                        problem[0] = e;
                                    }
                                }
                            });

                            mover.start();
                            synchronized (start) {
                                try {
                                    start.wait();
                                } catch (InterruptedException e) {
                                    //ok
                                }
                            }

                            IOException test = null;
                            try {
                                f1.asFile().moveTo(f2);
                            } catch (IOException e) {
                                test = e;
                            }

                            try {
                                mover.join();
                            } catch (InterruptedException e) {
                                //ok
                            }

                            if (problem[0] != null && test != null) {
                                logLN("Double rollback - ok");
                            } else  if (problem[0] != null) {
                                logLN("Rollback 1");
                                f2.getChildFolder("" + ii[0] + "1");
                            } else if (test != null) {
                                logLN("Rollback 2");
                                f1_1_1.getChildFolder("" + ii[0] + "2");
                            } else {
                                throw new IOException("Lost subtree");
                            }

                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                };//Runnable
                executor.execute(worker);
            }

            try {
                Thread.sleep(1000);
                log(root1.getView());
                ffs.waitForShutdown();
            } catch (InterruptedException ex) {
                //ok
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    //ok
                }
            }
        }

        //checkDirty()
        FATFileSystem.open(path).close();
        tearDown(path);
    }
    @Test
    public void testStressCrossChildMove() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE; //fixed!
        int clusterCount = 4000; //fixed!
        int allocatorType = allocatorTypes[0];

        logStart(getPath(), clusterSize, clusterCount, allocatorType);
        testStressCrossChildMove(getPath(), clusterSize, clusterCount, allocatorType);
        logOk();
    }

}
