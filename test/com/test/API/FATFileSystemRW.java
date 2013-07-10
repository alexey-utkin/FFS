package com.test.API;

/**
 * Basic File System Read Write operations.
 */

import com.test.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class FATFileSystemRW extends FATBaseTest {

    //
    //  Test of FS read-write basic
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

                final ByteBuffer rbf = ByteBuffer.allocateDirect(lbf.position() + 12);
                longFile.position(0);
                longFile.read(rbf);

                if (rbf.position() != longFile.size())
                    throw new Error("Wrong content read (lost EOF?)");

                rbf.flip();
                lbf.flip();
                while (rbf.hasRemaining()) {
                    if (rbf.get() != lbf.get())
                        throw new Error("Wrong content read.");
                }
                log(" Disk Size WR:Ok");

                long freeSize = ffs.getFreeSize();

                lbf.position(0);
                lbf.limit((int) (freeSize + 1));
                longFile.position(longFile.size());

                try {
                    longFile.write(lbf);
                    throw new Error("Write more than disk.");
                } catch (IOException ex) {
                    log(", Disk Size+1 W:Ok");
                }

                lbf.clear();
                longFile.position(longFile.size());
                //EOF test
                if (longFile.read(lbf) != -1)
                    throw new Error("Read more than file?");
                log(", File Size+1 R:Ok");

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
    //  Test of Lost Write Test for append mode
    //
    static final int MAX_FILE_SIZE = 0x64000; //1M = 1024*1024
    static public void testLostWrite(Path path, int clusterSize, int clusterCount,
                                     int allocatorType) throws IOException
    {
        startUp(path);

        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            final FATFile file = ffs.getRoot().createFile("longFile");

            int buffSize = (int) (ffs.getFreeSize()/6);
            final ByteBuffer bufferA =  ByteBuffer.allocateDirect(buffSize);
            while (bufferA.hasRemaining())
                bufferA.put((byte) 'A');

            ByteBuffer bufferB = ByteBuffer.allocateDirect(buffSize); //cluster size
            while (bufferB.hasRemaining())
                bufferB.put((byte) 'B');

            final Object writerAStart = new Object();
            final IOException problem[] = new IOException[]{null};
            Thread writerA = new Thread(new Runnable() {
                @Override public void run() {
                    try (FATFileChannel longFile = file.getChannel(true)) {
                        synchronized (writerAStart) {
                            writerAStart.notify();
                        }
                        for(int i = 0; i < 3; ++i) {
                            bufferA.flip();
                            while (bufferA.hasRemaining()) {
                                FATLock lock = file.getLock(true);
                                try {
                                    longFile.write(bufferA);
                                } finally {
                                    lock.unlock();
                                }
                            }
                        }
                    } catch (IOException e) {
                        problem[0] = e;
                    }
                }
            });

            writerA.start();
            synchronized (writerAStart) {
                try {
                    writerAStart.wait();
                } catch (InterruptedException e) {
                    //ok
                }
            }

            for(int i = 0; i < 3; ++i) {
                try (FATFileChannel longFile = file.getChannel(true)) {
                    bufferB.flip();
                    while (bufferB.hasRemaining()) {
                        FATLock lock = file.getLock(true);
                        try {
                            longFile.write(bufferB);
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }

            try {
                writerA.join();
                Thread.sleep(1);
                if (problem[0] != null) {
                    problem[0].printStackTrace();
                    throw new Error("Long buffer write problem.");
                }

                try (FATFileChannel longFile = file.getChannel(false)) {
                    boolean eof = false;
                    long countB = 0;
                    long countA = 0;
                    while (!eof) {
                        bufferB.clear();
                        while (bufferB.hasRemaining()) {
                            if (longFile.read(bufferB) < 0) {
                                eof = true;
                                break;
                            }
                        }
                        bufferB.flip();
                        while (bufferB.hasRemaining()) {
                            char c = (char) bufferB.get();
                            if ('B' == c)
                                ++countB;
                            else if ('A' == c)
                                ++countA;
                        }
                    }
                    if (countB != buffSize*3 || countA != buffSize*3)
                        throw new Error("Lost write problem.");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

//        tearDown(path);
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

    //
    //  Test of FS read-write basic
    //
    static public void testBaseFileCopy(Path path, int clusterSize, int clusterCount,
                                         int allocatorType) throws IOException
    {
        startUp(path);

        try (final FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            final ByteBuffer lbf =  ByteBuffer.allocateDirect((int)ffs.getFreeSize());
            {
                byte i = (byte)'0';
                while (lbf.hasRemaining()) {
                    lbf.put(i);
                    if (i == (byte)'A')
                        i = (byte)'0';
                    ++i;
                }
            }
            lbf.flip();

            FATFile src = ffs.getRoot().createFile("longFile");
            try (FATFileChannel longFile = src.getChannel(false)) {
                int startSize = (int)ffs.getFreeSize() - 3*FATFile.RECORD_SIZE;
                lbf.limit(startSize);
                longFile.write(lbf);
                log("Write long:Ok");

                try {
                    src.copyTo(ffs.getRoot(), true);
                    throw new Error("Copy to self");
                } catch (IOException ex) {
                    //ok
                    logLN("no copy to self:" + ex.getMessage());
                }

                FATFolder dstFolder = ffs.getRoot().createFolder("dst");
                FATFile dst = dstFolder.createFile("longFile");

                try {
                    src.copyTo(dstFolder, false);
                    throw new Error("Copy to existent");
                } catch (IOException ex) {
                    //ok - file exist
                    logLN("no copy to existence:" + ex.getMessage());
                }

                try {
                    src.copyTo(dstFolder, true);
                    throw new Error("Copy while no size");
                } catch (IOException ex) {
                    //ok - no space for copy
                    logLN("no space for copy:" + ex.getMessage());
                }

                longFile.truncate(startSize/2);
                src.copyTo(dstFolder, true);

                if (dst.length() != src.length())
                    throw new Error("Bad copy size");


                final ByteBuffer sbf =  ByteBuffer.allocateDirect((int)dst.length());
                try (FATFileChannel shortFile = src.getChannel(false)) {
                    shortFile.read(sbf);
                }
                sbf.flip();
                lbf.position(0);
                lbf.limit((int)dst.length());
                while (sbf.hasRemaining()) {
                    if (sbf.get() != lbf.get())
                        throw new Error("Bad copy context");
                }

                dst.delete();
                //test create copy
                src.copyTo(dstFolder, false);
            }
        }
        tearDown(path);
    }
    @Test
    public void testBaseFileCopy() throws IOException {
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
                    testBaseFileCopy(getPath(), clusterSize, clusterCount, allocatorType);
                    logOk();
                }
            }
        }
    }
}
