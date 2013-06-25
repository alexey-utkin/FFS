package com.test;

import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;

/**
 * Created with IntelliJ IDEA.
 * User: u
 * Date: 6/22/13
 * Time: 10:25 AM
 * To change this template use File | Settings | File Templates.
 */

class FATSystem implements Closeable {
    //file system header with magic number abd etc
    final static int  HEADER_SIZE = 16;
    final static int  VERSION     = 1;
    final static long MAPFILE_SIZE_LIMIT = Integer.MAX_VALUE;

    final static int FAT_E_SIZE  = 4; //bytes for FAT32
    final static int MAGIC_WORD  = 0x73666175; //check ENDIAN sfau/uafs as BIG/LITTLE

    final static int CLUSTER_INDEX  = 0x3FFFFFFF;
    final static int CLUSTER_STATUS = 0xC0000000;
    final static int CLUSTER_FREE      = 0x00000000;
    final static int CLUSTER_ALLOCATED = 0x40000000;
    final static int CLUSTER_EOC       = 0xC0000000;
    // Diagnostic
    final static int CLUSTER_UNUSED    = 0x0BADBEAF;
    final static int CLUSTER_DEALLOC   = 0x0CCCCCCC;

    //header
    private int fsVersion;
    private int clusterSize;
    private int clusterCount;

    //denomalized values
    private int entryPerCluster;
    private int fatOffset;
    private int dataOffset;

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer fatZone;
    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN; //default encoding
    private final Object lockFAT = new Object();
    private final Object lockData = new Object();

    private FATSystem() {}

    /**
     * Creates new file-based file system.
     * @param path is the parent FS path to the file that need be created
     * @param clusterSize  the size of single cluster. Mast be at least [FolderEntry.RECORD_SIZE] size
     * @param clusterCount the total number of clusters in the system.
     * @return new In-file File System over the created file in host File System.
     * @throws FileSystemException for bad parameters or file access problem in the host system
     */
    public static FATSystem create(Path path, int clusterSize, int clusterCount) throws IOException {
        if (clusterSize < FolderEntry.RECORD_SIZE)
            throw new FileSystemException("Bad value of cluster size:" + clusterSize);

        if (clusterCount <= 0 || clusterCount > CLUSTER_INDEX || clusterCount*FAT_E_SIZE > MAPFILE_SIZE_LIMIT)
            throw new FileSystemException("Bad value of cluster count:" + clusterCount);

        long length = (long)clusterCount * clusterSize;
        if (length/clusterSize != clusterCount)
            throw new FileSystemException("File system is too big. FAT overloaded.");

        long sizeFS = length + HEADER_SIZE;
        if (sizeFS < length)
            throw new FileSystemException("File system is too big. No space for header." );

        FATSystem ret = new FATSystem();
        boolean success = false;
        try {
            ret.randomAccessFile = new RandomAccessFile(path.toString(), "rw");
            ret.randomAccessFile.setLength(sizeFS);
            ret.initStorage(path, clusterSize, clusterCount);
            success = true;
        } finally {
            if (!success)
                ret.close();
        }
        return ret;
    }

    private void initDenormalized() {
        entryPerCluster = clusterSize/FolderEntry.RECORD_SIZE;
        fatOffset = HEADER_SIZE; //version dependant
        dataOffset = fatOffset + clusterCount*FAT_E_SIZE;
    }

    private ByteBuffer allocateBuffer(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(byteOrder);
    }

    private void writeToChannel(ByteBuffer bf) throws IOException {
        bf.flip();
        while(bf.hasRemaining()) {
            fileChannel.write(bf);
        }
    }

    private void initStorage(Path path, int _clusterSize, int _clusterCount) throws IOException {

        fsVersion = 1;
        clusterSize = _clusterSize;
        clusterCount = _clusterCount;
        initDenormalized();

        fileChannel = randomAccessFile.getChannel();

        writeToChannel(allocateBuffer(HEADER_SIZE)
            // init header
            .putInt(MAGIC_WORD)
            .putInt(fsVersion)     //FS version
            .putInt(clusterSize)
            .putInt(clusterCount));

        // map FAT section
        fatZone = fileChannel.map(FileChannel.MapMode.READ_WRITE, fatOffset, clusterCount*FAT_E_SIZE);
        fatZone.order(byteOrder);
        // init FAT32
        for (int i = 0; i < clusterCount; ++i) {
            fatZone.putInt(CLUSTER_UNUSED);
        }

        // init Data Region
        //createRoot();

        flush();
    }

    @Override
    public void close() throws IOException {
        if (randomAccessFile != null) {
            if (fileChannel != null) {
                if (fatZone != null) {
                    try {
                        // That is bad, but it is the only available solution
                        // http://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java
                        sun.misc.Cleaner cleaner = ((DirectBuffer)fatZone).cleaner();
                        cleaner.clean();
                    } catch (Throwable any) {
                        System.err.println();
                    }
                }
                fileChannel.close();
            }
            randomAccessFile.close();
        }
    }

    public void flush() throws IOException {
        // One is not a guaranty for another
        fileChannel.force(true);
        //fatZone.force();
    }

    int getVersion() {
       return fsVersion;
    }

    int getEntryPerCluster() {
        return entryPerCluster;
    }

    public ByteBuffer readCluster(int cluster) throws IOException {
        if (cluster < 0 || cluster >= clusterCount)
            throw new FileSystemException("Bad cluster index:" + cluster);

        ByteBuffer bf = ByteBuffer.allocateDirect(clusterSize);  //check with alloc!
        synchronized (lockData) {
            fileChannel
                .position(dataOffset + cluster * clusterSize)
                .read(bf);
        }
        return bf;
    }

    /**
     * Reads FAT entry by index.
     * @param cluster the index of current cluster in the chain if any
     * @return the entry value
     */
    public int readFatEntry(int cluster) {
        synchronized (lockFAT) {
            return fatZone.getInt(cluster*FAT_E_SIZE);
        }
    }

    /**
     * Allocates a cluster chain.
     * @param findAfter the index of the tail of the chain.
     *        The -1 value means that the chain should not be joined.
     *        Any other value means that allocated cain will join to [findAfter] tail
     * @param count the number of cluster in the returned chain
     * @return the index of the first cluster in allocated chain.
     * @throws FileSystemException if the chain could not be allocated
     */
    int allocateClusters(int findAfter, int count) throws FileSystemException {
        //fat lock
        // bitmap of free clusters in memory?
        // chunk with free block counting?
        if (count < 1)
            throw new FileSystemException("Cannot allocate" + count + "clusters.");
        if ((findAfter < clusterCount) && (count <= clusterCount)) {
            synchronized (lockFAT) {
                int currentOffset = (findAfter == -1)
                    ? 0
                    : findAfter + 1;
                int headOffset = -1;
                int tailOffset = findAfter;
                int endOfLoop = currentOffset; // loop marker
                while (true) {
                    if (currentOffset >= clusterCount) {
                        currentOffset = 0;
                        if (currentOffset == endOfLoop)
                            break;
                    }

                    int fatEntry = fatZone.getInt(currentOffset*FAT_E_SIZE);
                    if ((fatEntry & CLUSTER_STATUS) == CLUSTER_FREE) {
                        if (headOffset == -1)
                            headOffset = currentOffset;

                        // mark as EOC
                        fatZone.putInt(currentOffset*FAT_E_SIZE, CLUSTER_EOC);
                        if (tailOffset != -1) {
                            // mark as ALLOCATED with forward index
                            fatZone.putInt(tailOffset*FAT_E_SIZE, CLUSTER_ALLOCATED | currentOffset);
                        }
                        tailOffset = currentOffset;
                        --count;
                        if (count == 0)
                            return headOffset;
                    }
                    ++currentOffset;
                    if (currentOffset == endOfLoop)
                        break;
                }
                // rollback allocation.
                if (findAfter == -1)
                    freeClusters(headOffset, true);
                else
                    freeClusters(findAfter, false);
            }
        }
        throw new FileSystemException("Disk full.");
    }

    /**
     * Frees chain that start from [headOffset] cluster.
     * @param headOffset the head of the chain
     * @param freeHead if [true] the chain is freed together with head cluster
     *                 else head cluster is marked as [EOC]
     * @throws FileSystemException
     */
    private void freeClusters(int headOffset, boolean freeHead) throws FileSystemException {
        synchronized (lockFAT) {
            if (!freeHead) {
                int value = fatZone.getInt(headOffset*FAT_E_SIZE);
                // CLUSTER_ALLOCATED only
                if ((value & CLUSTER_STATUS) == CLUSTER_ALLOCATED) {
                    // mark as EOC
                    fatZone.putInt(headOffset*FAT_E_SIZE, CLUSTER_EOC);
                    headOffset = value & CLUSTER_INDEX;
                } else {
                    throw new FileSystemException("Cluster double free in tail.  Cluster#:" + headOffset
                            + " Value:" + value);
                }
            }
            while (true) {
                int value = fatZone.getInt(headOffset*FAT_E_SIZE);
                // CLUSTER_ALLOCATED or CLUSTER_EOC
                if ((value & CLUSTER_ALLOCATED) == CLUSTER_ALLOCATED) {
                    // mark as DEALLOC
                    fatZone.putInt(headOffset*FAT_E_SIZE, CLUSTER_DEALLOC);
                    if ((value & CLUSTER_EOC) == CLUSTER_EOC)
                        break;
                    headOffset = value & CLUSTER_INDEX;
                } else {
                    throw new FileSystemException("Cluster double free. Cluster#:" + headOffset
                            + " Value:" + value);
                }
            }
        }
    }

    public long getCurrentTime() {
        return System.currentTimeMillis();
    }

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
            final int circleCount = 5;

            /* random size allocation */
            final int[] fragmentLengths = new int[] {
                    clusterCount/7,
                    clusterCount/5,
                    clusterCount/3,
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
            for (int i = 0; i < fragmentLengths.length; ++i) {
                final int actionI = i;
                actions[i] = new Thread(new Runnable() {
                    @Override public void run() {
                        for (int k = 0; k < circleCount; ++k) {
                            try {
                                int start = ffs.allocateClusters(-1, fragmentLengths[actionI]);
                                Thread.sleep(1); // switch thread
                                if (k + 1 != circleCount)
                                    ffs.freeClusters(start, true);
                            } catch (Throwable e) {
                                errors[actionI] = e;
                            }
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
