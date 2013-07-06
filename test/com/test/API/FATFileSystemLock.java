package com.test.API;

/**
 * Complex File System Tree operations.
 */

import com.test.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

public class FATFileSystemLock extends FATBaseTest {
    //
    // Test of FS open.
    //
    static public void testBlockedDelete(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException {
        startUp(path);

        try (final FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            final FATFolder container  = ffs.getRoot().createFolder("container");
            final Throwable problem[] = new Throwable[1];
            final Object started = new Object();
            Thread worker = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        FATFile context = container.createFile("context");
                        FATLock lock = context.getLock(false);
                        synchronized (started) {
                            started.notify();
                        }
                        try {
                            Thread.sleep(300);
                        } finally {
                            lock.unlock();
                        }
                    } catch (Throwable r) {
                        r.printStackTrace();
                    }
                }
            });
            worker.start();

            synchronized (started) {
                try {
                    started.wait();
                } catch (InterruptedException e) {
                    //ok
                }
            }

            try {
                ffs.getRoot().deleteChildren();
                throw new Error("Locked delete");
            } catch (FATFileLockedException ex) {
                //ok
            }

            try {
                worker.join();
            } catch (InterruptedException e) {
                //ok
            }
        }

        tearDown(path);
    }
    @Test
    public void testBlockedDelete() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE; //fixed!
        int clusterCount = 400; //fixed!
        int allocatorType = allocatorTypes[0];

        logStart(getPath(), clusterSize, clusterCount, allocatorType);
        testBlockedDelete(getPath(),
                clusterSize, clusterCount, allocatorType);
        logOk();
    }

}
