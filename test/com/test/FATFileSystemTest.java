package com.test;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 */

public class FATFileSystemTest  extends FATBaseTest {
/*
    //
    // Test of FS creation.
    //
    static public void testFileCreate(Path path, int clusterSize, int clusterCount,
                                      int allocatorType) throws IOException {
        startUp(path);
        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {

        }
        tearDown(path);
    }
    @Test
    public void testFileCreate() throws IOException {
        final int[] clusterSizes = new int[] {
            FATFile.RECORD_SIZE,
            4096
        };
        int clusterCount = 2;
        for (int allocatorType : allocatorTypes) {
            for (int clusterSize : clusterSizes) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testFileCreate(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }

    //
    // Test of forward space reservation in folder store.
    //
    static public void testForwardFolderReservation(Path path, int clusterSize, int clusterCount,
                                                    int allocatorType) throws IOException {
        startUp(path);
        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root1 = ffs.getRoot();
            //here: 2 free, 1 <root:empty> record is out of FAT

            try {
                root1.createFolder("Test1");
                //here: 1 free, 1 <root:full>, 1 <Test1:empty>
                root1.createFolder("Test2");
                //root is full, 2 free clusters need
                throw new Error("Impossible allocation.");
            } catch (IOException ex) {
                root1.pack();
                //check that we does not loose the space.
                if (ffs.getFreeSize() != FATFile.RECORD_SIZE)
                    throw new Error("Lost cluster on folder allocation.");
                root1.findFile("Test1").getFolder().createFolder("Test2");
                //here: 0 free, 1 <root:full>, 1 <Test1:full>, 1 <Test2:empty>
                if (ffs.getFreeSize() != 0)
                    throw new Error("Lost cluster on folder allocation 2.");
            }
        }
        tearDown(path);
    }
    @Test
    public void testForwardFolderReservation() throws IOException {
        for (int allocatorType : allocatorTypes) {
            int clusterSize = FATFile.RECORD_SIZE; //fixed!
            int clusterCount = 3; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testForwardFolderReservation(getPath(),
                    clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

    //
    // Test of FS open.
    //
    static public void testFileOpen(Path path, int clusterSize, int clusterCount,
                                      int allocatorType) throws IOException {
        startUp(path);

        final FATFileSystem ffs1  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType);
        FATFolder root1 = ffs1.getRoot();
        root1.createFolder("Test1");
        root1.createFolder("Test2");
        root1.createFolder("Test3");

        try {
            // test check name first, available space size
            root1.createFolder("Test2");
            throw new Error("Can create duplicates.");
        } catch (FileAlreadyExistsException ex) {
            //OK
        }

        ffs1.close();

        final FATFileSystem ffs2  = FATFileSystem.open(path);
        FATFolder root2 = ffs2.getRoot();
        if (root2.childFiles.isEmpty())
            throw new Error("Bad root.");
        ffs2.close();

        tearDown(path);
    }
    @Test
    public void testFileOpen() throws IOException {
        final int[] clusterSizes = new int[] {
                FATFile.RECORD_SIZE,
                4096
        };
        // = 1dir-empty + 1rec-in-root * 3folders count
        int clusterCount = 6; //fixed in minimal value
        for (int allocatorType : allocatorTypes) {
            for (int clusterSize : clusterSizes) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testFileOpen(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }


    //
    //  Test of FS read-write .
    //
    static public void testBaseReadWrite(Path path, int clusterSize, int clusterCount,
                                        int allocatorType) throws IOException
    {
        startUp(path);

        try (final FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            final ByteBuffer lbf =  ByteBuffer.allocateDirect((int) (ffs.getFreeSize() + 100));
            {
                byte i = (byte)'0';
                while (lbf.hasRemaining()) {
                    lbf.put(i);
                    if (i == (byte)'A')
                        i = (byte)'0';
                    ++i;
                }
            }

            lbf.limit((int) ffs.getFreeSize());
            try (FATFileChannel longFile = ffs.getRoot().createFile("longFile").getChannel(false)) {
                lbf.flip();
                longFile.write(lbf);

                lbf.flip();
                longFile.read(lbf);
                byte i = (byte)'0';
                while (lbf.hasRemaining()) {
                    if (i != lbf.get())
                        throw new Error("Wrong content read.");
                    if (i == (byte)'A')
                        i = (byte)'0';
                    ++i;
                }
                log(" Disk Size WR:Ok");

                lbf.flip();
                lbf.limit((int)ffs.getFreeSize() + 1);
                try {
                    longFile.write(lbf);
                    throw new Error("Write more than disk.");
                } catch (IOException ex) {
                    //OK: disk full
                    log(", Disk Size+1 W:Ok");
                }

                if (longFile.read(lbf) >= 0)
                    throw new Error("Read more than disk");
                log(", Disk Size+1 R:Ok");

                log(", common:");
            }
        }

        tearDown(path);
    }
    @Test
    public void testBaseReadWrite() throws IOException {
        int[] clusterCounts = new int[] {
                31, 1021, 4096
        };
        int[] clusterSizes = new int[] {
            FATFile.RECORD_SIZE + 17, FATFile.RECORD_SIZE*3
        };
        for (int allocatorType : allocatorTypes) {
             for(int clusterCount : clusterCounts) {
                 for(int clusterSize : clusterSizes) {
                    logStart(getPath(), clusterSize, clusterCount, allocatorType);
                    testBaseReadWrite(getPath(), clusterSize, clusterCount, allocatorType);
                    logOk();
                 }
             }
        }
    }

    //
    //  Test of FS tree test.
    //
    static public void testBaseTree(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException {
        startUp(path);

        final FATFileSystem ffs1  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType);
        FATFolder root1 = ffs1.getRoot();
        root1.createFolder("Test1")
             .createFolder("Test1_2")
             .createFolder("Test1_3");

        root1.deleteChildren();
        if (root1.findFile("Test1") != null)
            throw new Error("Bad deleteChildren call.");

        root1.createFolder("Test2");
        root1.createFolder("Test3");
        root1.pack();
        
        ffs1.close();

        final FATFileSystem ffs2  = FATFileSystem.open(path);
        FATFolder root2 = ffs2.getRoot();
        if (root2.childFiles.size() != 2)
            throw new Error("Bad root.");
        ffs2.close();

        tearDown(path);
    }
    @Test
    public void testBaseTree() throws IOException {
        for (int allocatorType : allocatorTypes) {
            int clusterSize = FATFile.RECORD_SIZE*3/2; //fixed!
            int clusterCount = 400; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testBaseTree(getPath(), clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }
*/
    //
    //  Test of FS tree test.
    //
    static int MAX_FILE_SIZE = 0x64000; //1M = 1024*1024
    static public void testLostWrite(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException
    {
        startUp(path);

        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            final FATFile file = ffs.getRoot().createFile("longFile");
            final ByteBuffer bufferA =  ByteBuffer.allocateDirect((int) (ffs.getFreeSize() - 4096*3));
            while (bufferA.hasRemaining())
                bufferA.put((byte) 'A');

            ByteBuffer bufferB = ByteBuffer.allocateDirect(4096); //cluster size
            while (bufferB.hasRemaining())
                bufferB.put((byte) 'B');

            final IOException problem[] = new IOException[]{null};
            Thread writerA = new Thread(new Runnable() {
                @Override public void run() {
                    bufferA.flip();
                    try (FATFileChannel longFile = file.getChannel(false)) {
                        while (bufferA.hasRemaining())
                            longFile.write(bufferA);
                    } catch (IOException e) {
                        problem[0] = e;
                    }
                }
            });

            //writerA.start();
            for(int i = 0; i < 3; ++i) {
                try (FATFileChannel longFile = file.getChannel(true)) {
                    bufferB.flip();
                    while (bufferB.hasRemaining())
                        longFile.write(bufferB);
                    ++i;
                }
            }

            try {
                //writerA.join();
                Thread.sleep(1);
                if (problem[0] != null) {
                    problem[0].printStackTrace();
                    throw new Error("Long buffer write problem.");
                }

                try (FATFileChannel longFile = file.getChannel(false)) {
                    boolean eof = false;
                    long countB = 0;
                    while (eof) {
                        bufferB.position(0);
                        while (bufferB.hasRemaining()) {
                            if (longFile.read(bufferB) < 0) {
                                eof = true;
                                break;
                            }
                        }
                        bufferB.flip();
                        while (bufferB.hasRemaining()) {
                            if ('B' == bufferA.get())
                                ++countB;
                        }
                    }
                    if (countB != 4096*3)
                        throw new Error("Lost write problem.");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        tearDown(path);
    }
    @Test
    public void testLostWrite() throws IOException {
        for (int allocatorType : allocatorTypes) {
            int clusterSize = 4096; //fixed!
            int clusterCount = MAX_FILE_SIZE/clusterSize; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testLostWrite(getPath(), clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

/*
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
*/
}
