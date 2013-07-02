package com.test.API;

/**
 * Stress test fo File System.
 * User: uta
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


        final FATFileSystem ffs2  = FATFileSystem.open(path);
        FATFolder root2 = ffs2.getRoot();
        ffs2.close();

        tearDown(path);
    }
    
    @Test(timeout=66660000)
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
}
