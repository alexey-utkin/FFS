package com.test.API;

/**
 * Complex File System Tree operations.
 */

import com.test.*;
import org.junit.Test;

import java.io.IOException;
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

    //
    //  Test of FS tree move.
    //
    static public void testMoveChild(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException {
        startUp(path);

        String dump1;
        try (final FATFileSystem ffs = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root1 = ffs.getRoot();

            //1->1_1->1_1_1
            FATFolder f1_1 = root1.createFolder("1").createFolder("1_1");
            FATFolder f1_1_1 = f1_1.createFolder("1_1_1");

            try {
                f1_1.asFile().moveTo(f1_1_1);
                throw new Error("Move to child.");
            } catch (IOException ex) {
                //ok
            }

            try {
                f1_1.asFile().moveTo(f1_1);
                throw new Error("Move to self.");
            } catch (IOException ex) {
                //ok
            }

            f1_1.asFile().moveTo(
                 root1.createFolder("2"));

            log(root1.getView());
        }
        tearDown(path);
    }
    @Test
    public void testMoveChild() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE; //fixed!
        int clusterCount = 400; //fixed!
        int allocatorType = allocatorTypes[0];

        logStart(getPath(), clusterSize, clusterCount, allocatorType);
        testMoveChild(getPath(), clusterSize, clusterCount, allocatorType);
        logOk();
    }

    //
    //  Test of cross move.
    //
    static public void testCrossMove(Path path, int clusterSize, int clusterCount,
                                     int allocatorType) throws IOException {
        startUp(path);

        String dump1;
        try (final FATFileSystem ffs = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root1 = ffs.getRoot();

            //1->1_1->1_1_1
            final FATFolder f1_1 = root1.createFolder("1").createFolder("1_1");
            final FATFolder f2 = root1.createFolder("2");
            final FATFolder f1_1_1 = f1_1.createFolder("1_1_1");

            final Object start = new Object();
            final IOException problem[] = new IOException[]{null};
            Thread mover = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        synchronized (start) {
                            start.notify();
                        }
                        f2.asFile().moveTo(f1_1);
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
                f1_1.asFile().moveTo(f2);
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
                f2.getChildFolder("1_1");
            } else if (test != null) {
                logLN("Rollback 2");
                f1_1.getChildFolder("2");
            } else {
                throw new IOException("Lost subtree");
            }
            log(root1.getView());
        }
        tearDown(path);
    }
    @Test
    public void testCrossMove() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE; //fixed!
        int clusterCount = 400; //fixed!
        int allocatorType = allocatorTypes[0];

        logStart(getPath(), clusterSize, clusterCount, allocatorType);
        testCrossMove(getPath(), clusterSize, clusterCount, allocatorType);
        logOk();
    }



    //
    //  Test of cross child move.
    //
    static public void testCrossChildMove(Path path, int clusterSize, int clusterCount,
                                     int allocatorType) throws IOException {
        startUp(path);

        String dump1;
        try (final FATFileSystem ffs = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root1 = ffs.getRoot();

            //1->1_1->1_1_1
            final FATFolder f1 = root1.createFolder("1");
            final FATFolder f1_1 = f1.createFolder("1_1");
            final FATFolder f1_1_1 = f1_1.createFolder("1_1_1");
            final FATFolder f2 = root1.createFolder("2");

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
                f2.getChildFolder("1");
            } else if (test != null) {
                logLN("Rollback 2");
                f1_1_1.getChildFolder("2");
            } else {
                throw new IOException("Lost subtree");
            }

            log(root1.getView());
        }
        tearDown(path);
    }
    @Test
    public void testCrossChildMove() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE; //fixed!
        int clusterCount = 400; //fixed!
        int allocatorType = allocatorTypes[0];

        logStart(getPath(), clusterSize, clusterCount, allocatorType);
        testCrossChildMove(getPath(), clusterSize, clusterCount, allocatorType);
        logOk();
    }

}
