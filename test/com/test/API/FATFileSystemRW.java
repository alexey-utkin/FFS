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
            try (FATFileChannel longFile = ffs.getRoot().createFile("longFile").getChannel(false, true)) {
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
    //  Test of Lost Write Test for append mode
    //
    static int MAX_FILE_SIZE = 0x64000; //1M = 1024*1024
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
                    try (FATFileChannel longFile = file.getChannel(true, true)) {
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
                try (FATFileChannel longFile = file.getChannel(true, true)) {
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

                try (FATFileChannel longFile = file.getChannel(false, true)) {
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

}
