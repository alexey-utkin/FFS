package com.test;

import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 *
 * Max storage size for 4k cluster: CLUSTER_INDEX*4096 = 3FF FFFF F000
 * 0x3FFFFFFF000/0x10000000000 = 3T - big enough.
 */


class FATSystem implements Closeable {
    final static int ALLOCATOR_CLASSIC_HEAP = 0;
    final static int ALLOCATOR_FAST_FORWARD = 1;

    //file system header with magic number abd etc
    final static int  HEADER_SIZE = 32;
    final static int  FREE_CLUSTER_COUNT_OFFSET = 5*4;
    final static int  VERSION     = 1;
    final static long MAPFILE_SIZE_LIMIT = Integer.MAX_VALUE;

    final static int FAT_E_SIZE  = 4; //bytes for FAT32
    final static int MAGIC_WORD  = 0x75616673;
    //final static int MAGIC_WORD  = 0x73666175; //check ENDIAN sfau/uafs as BIG/LITTLE

    //header
    private int fsVersion;
    private int clusterSize;
    int clusterCount;

    //denomalized values
    private int entryPerCluster;
    private int fatOffset;
    private int dataOffset;
    int freeClusterCount; //can get [-1] on "dirty FAT"

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer fatZone;
    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN; //default encoding (currently fixed)
    private boolean opened = true; //allocate opened storage, check status in fabric
    private FATClusterAllocator clusterAllocator;

    // lockFAT -> lockData lock sequence
    private final Object lockFAT = new Object();
    private final Object lockData = new Object();
    private final boolean normalMode;

    /**
     * Returns the FS mode.
     * @return [true] means throwing the [IOException] exception when "dirty FAT" is detected.
     *         [false] means means maintenance mode.
     */
    public boolean isNormalMode() {
        return normalMode;
    }

    /**
     * Opens FAT from the exist file in host FS.
     * @param path the path to storage file in host FS
     * @param normalMode the [false] value allows to open dirty FAT for maintenance
     * @return In-file FS over the file that opened in host FS.
     */
    static FATSystem open(Path path, boolean normalMode) throws IOException {
        if (Files.notExists(path))
            throw new FileNotFoundException();
        FATSystem ret = new FATSystem(normalMode);
        boolean success = false;
        try {
            ret.randomAccessFile = new RandomAccessFile(path.toString(), "rw");
            ret.initFromFile();
            success = true;
        } finally {
            if (!success)
                ret.close();
        }
        return ret;
    }

    /**
     * Checks FS for [opened] state.
     * @return the state, [true] if FS is in action.
     */
    public boolean isOpen() {
        return opened;
    }

    /**
     * Restores FS from storage file. Works in exclusive mode.
     * @throws IOException
     */
    private void initFromFile() throws IOException {
        if (randomAccessFile.length() < HEADER_SIZE)
            throw new IOException("Wrong media size. File is too short.");

        fileChannel = randomAccessFile.getChannel();
        ByteBuffer bf = allocateBuffer(HEADER_SIZE);
        readFromChannel(bf);

        // init header
        int magic = bf.getInt(); //media type
        if (magic != MAGIC_WORD)
            throw new IOException("Wrong media type. That is not FFS file");
        int version = bf.getInt();  //FS version
        if (version != VERSION)
            throw new IOException("Wrong version: " + version
                      + "Version " +  VERSION + " is the only supported.");
        int allocatorType = bf.getInt();
        clusterSize = bf.getInt();
        clusterCount = bf.getInt();
        freeClusterCount = bf.getInt();
        if (freeClusterCount < 0)
            LogError("Open for read-only. Dirty state.");

        // max storage size for 4k cluster: CLUSTER_INDEX*4096 = 3FF FFFF F000
        // 0x3FFFFFFF000/0x10000000000 = 3T - big enough.
        long sizeFS = getRequestedStorageFileSize(clusterSize, clusterCount);

        if (randomAccessFile.length() < sizeFS) {
            setDirtyStatus("Wrong storage size. Storage was truncated in host FS.");
        }
        
        initDenormalized();
        // map FAT section
        fatZone = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fatOffset + clusterCount*FAT_E_SIZE);
        clusterAllocator = createAllocator(allocatorType);
        clusterAllocator.initFromFile();
        //set dirty status
        writeFreeClusterCount(-1);
        forceFat();
    }

    /**
     * Creates new FAT file system.
     * @param path is the path in host FS for file storage that need be created
     * @param clusterSize  the size of single cluster. Mast be at least [FATFile.RECORD_SIZE] size
     * @param clusterCount the total number of clusters in created file storage.
     * @param allocatorType the cluster allocation strategy
     * @return new In-file FS over the file that created in host FS.
     * @throws IOException for bad parameters or file access problem in the host FS
     */
    public static FATSystem create(Path path, int clusterSize,int clusterCount,
                                   int allocatorType) throws IOException {
        if (clusterSize < FATFile.RECORD_SIZE)
            throw new IOException("Bad value of cluster size:" + clusterSize);

        // max storage size for 4k cluster: CLUSTER_INDEX*4096 = 3FF FFFF F000
        // 0x3FFFFFFF000/0x10000000000 = 3T - big enough.
        long sizeFS = getRequestedStorageFileSize(clusterSize, clusterCount);

        FATSystem ret = new FATSystem(true);
        boolean success = false;
        try {
            ret.randomAccessFile = new RandomAccessFile(path.toString(), "rw");
            ret.randomAccessFile.setLength(sizeFS);
            ret.initStorage(clusterSize, clusterCount, allocatorType);
            success = true;
        } finally {
            if (!success)
                ret.close();
        }
        return ret;
    }

    /**
     * Initializes new storage from parameters.
     *
     * @param _clusterSize single cluster size
     * @param _clusterCount count of clusters in created storage
     * @param allocatorType the allocation algorithm for FAT
     * @throws IOException
     */
    private void initStorage(int _clusterSize, int _clusterCount,
                             int allocatorType) throws IOException {
        fsVersion = VERSION;
        clusterSize = _clusterSize;
        clusterCount = _clusterCount;
        initDenormalized();
        freeClusterCount = clusterCount;
        fileChannel = randomAccessFile.getChannel();

        // map FAT section
        fatZone = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fatOffset + clusterCount*FAT_E_SIZE);
        fatZone.order(byteOrder)
            // init header
            .putInt(MAGIC_WORD)
            .putInt(fsVersion)     //FS version
            .putInt(allocatorType)
            .putInt(clusterSize)
            .putInt(clusterCount)
            //Set dirty flag in free cluster count. We drop it on right close.
            .putInt(-1);
        clusterAllocator = createAllocator(allocatorType);
        clusterAllocator.initFAT();
        forceFat();
    }

    private void writeFreeClusterCount(int value) {
        if (isNormalMode()) {
            // saves real dirty status
            fatZone.position(FREE_CLUSTER_COUNT_OFFSET);
            fatZone.putInt(value);
        }
    }

    @Override
    public void close() throws IOException {
        boolean needGCrun = false;
        synchronized (lockFAT) {
            synchronized (lockData) {
                if (!opened)
                    throw new IOException("Storage was closed earlier.");
                try {
                    if (randomAccessFile != null) {
                        if (fileChannel != null) {
                            if (fatZone != null) {
                                writeFreeClusterCount(freeClusterCount);
                                if (fatZone instanceof DirectBuffer) {
                                    // That is bad, but it is the only available solution
                                    // http://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java
                                    sun.misc.Cleaner cleaner = ((DirectBuffer)fatZone).cleaner();
                                    cleaner.clean();
                                } else {
                                    fatZone = null;
                                    needGCrun = true;
                                    LogError("Not Oracle implementation for memory-mapped file."
                                           + "We can get a problem. Trying direct GC call.");
                                }
                            }
                            fileChannel.close();
                        }
                        randomAccessFile.close();
                    }
                } finally {
                    // RIP
                    opened = false;
                }
            }
        }
        if (needGCrun)
            System.gc();
    }

    public void forceChannel(boolean updateMetadata) throws IOException {
        fileChannel.force(updateMetadata);
    }

    public void force() throws IOException {
        // One is not a guaranty for another
        synchronized (lockData) {
            forceChannel(true);
        }
        synchronized (lockFAT) {
            forceFat();
        }
    }

    /**
     * Flush content to disk.
     * Have to be called in [lockFAT] section
     */
    private void forceFat() {
        clusterAllocator.force();
        fatZone.force();
    }


    int getVersion() {
       return fsVersion;
    }

    int getEntryPerCluster() {
        return entryPerCluster;
    }

    /**
     * Returns the capacity of FS
     * @return the size of storage. That is the [Data Section] size.
     */
    public long getSize() {
        return  clusterCount*clusterSize;
    }

    /**
     * Returns the free capacity of FS
     * @return the free size in storage. The [-1] means dirty FAT and the system needs in maintenance.
     */
    public long getFreeSize() {
        return freeClusterCount*clusterSize;
    }

    /**
     * Reads cluster content.
     * @param cluster the index of the cluster in FAT
     * @return cluster content
     * @throws IOException
     */
    public ByteBuffer readCluster(int cluster) throws IOException {
        if (cluster < 0 || cluster >= clusterCount)
            throw new IOException("Bad cluster index:" + cluster);

        ByteBuffer bf = ByteBuffer.allocateDirect(clusterSize);  //check with alloc!
        synchronized (lockData) {
            checkCanRead();
            fileChannel
                .position(dataOffset + cluster * clusterSize)
                .read(bf);
        }
        return bf;
    }

    /**
     * Reads FAT entry by index.
     *
     * @param cluster the index of current cluster in the chain if any
     * @return the entry value
     */
    public int readFatEntry(int cluster) throws IOException {
        synchronized (lockFAT) {
            checkCanRead();
            return fatZone.getInt(cluster*FAT_E_SIZE);
        }
    }

    /**
     * Allocates a cluster chain.
     *
     * @param tailCluster the index of the tail of the chain.
     *        The -1 value means that the chain should not be joined.
     *        Any other value means that allocated cain will join to [tailCluster] tail
     * @param count the number of cluster in the returned chain
     * @return the index of the first cluster in allocated chain.
     * @throws IOException if the chain could not be allocated
     */
    int allocateClusters(int tailCluster, int count) throws IOException {
        // - bitmap of free clusters in memory?
        // - chunk with free block counting for sequential allocation?
        // - two stage allocation with "gray"
        //    +final static int CLUSTER_GALLOCATED = 0x80000000;
        //    +final static int CLUSTER_GEOC       = 0xC8000000;
        //   up bit?
        // - resize if need?
        if (count < 1)
            throw new IOException("Cannot allocate" + count + "clusters.");
        synchronized (lockFAT) {
            checkCanWrite();
            if ((tailCluster < clusterCount) && (
                    ((freeClusterCount >= 0) && (count <= freeClusterCount))
                 || ((freeClusterCount  < 0) && (count <= clusterCount)))) // without guaranty on dirty FAT
            {
                try {
                    if (tailCluster != -1 && getFatEntry(tailCluster) != FATClusterAllocator.CLUSTER_EOC)
                        throw new IOException("Can join the chain with the tail only.");
                    return clusterAllocator.allocateClusters(tailCluster, count);
                } finally {
                    forceFat();
                }
            }
            throw new IOException("Disk full.");
        }
    }

    /**
     * Frees chain that start from [headOffset] cluster.
     *
     * @param headOffset the head of the chain
     * @param freeHead if [true] the chain is freed together with head cluster
     *                 else head cluster is marked as [EOC]
     * @throws IOException
     */
     void freeClusters(int headOffset, boolean freeHead) throws IOException {
        synchronized (lockFAT) {
            checkCanWrite();
            try {
                clusterAllocator.freeClusters(headOffset, freeHead);
            } finally {
                forceFat();
            }
        }
    }


    /**
     * Calculates the length of chain in clusters to hold [size] bytes.
     *
     * @param size the number of bytes to hold.
     * @return the length of the chain
     */
    int getSizeInClusters(long size) {
        if (size < clusterSize)
            return 1;
        int fullClusters = (int)(size/clusterSize);
        return (fullClusters*clusterSize == size)
            ? fullClusters
            : fullClusters + 1;
    }

    /**
     * Finds the [newSizeInClusters] value in the list that starts from [startCluster]
     *
     * Have to be called under [lockFAT] lock.
     *
     * @param startCluster the start of the chain
     * @param nextCount the number of [next] actions in list.
     * @return the index of cluster that is at [newSizeInClusters] pos in chain.
     */
    private int getShift(int startCluster, int nextCount) throws IOException {
        while (nextCount > 0) {
            int fatEntry = getFatEntry(startCluster);
            // CLUSTER_ALLOCATED only
            if ((fatEntry & FATClusterAllocator.CLUSTER_STATUS) == FATClusterAllocator.CLUSTER_ALLOCATED) {
                startCluster = fatEntry & FATClusterAllocator.CLUSTER_INDEX;
            } else {
                setDirtyStatus("Cluster chain is broken. Cluster#:" + startCluster
                        + " Value:" + fatEntry);
            }
        }
        return startCluster;
    }

    /**
     * Adjust cluster chain to hold [newLength] bytes.
     *
     * if [newLength] is [0], one cluster need to be allocated.
     *
     * @param startCluster the index of of the first cluster in chain
     * @param newLength the size in bytes to store in the chain
     */
    void adjustClusterChain(int startCluster, long newLength, long oldLength) throws IOException {
        synchronized (lockFAT) {
            checkCanWrite();
            // check only public parameters
            if (newLength < 0 || newLength > getSize())
                throw new IOException("Wrong FATFile size:" + newLength);
            int oldSizeInClusters = getSizeInClusters(oldLength);
            int newSizeInClusters = getSizeInClusters(newLength);
            // do nothing for [newSizeInClusters = oldSizeInClusters]
            if (newSizeInClusters < oldSizeInClusters) {
                freeClusters(getShift(startCluster, newSizeInClusters - 1), false);
            } else if (newSizeInClusters > oldSizeInClusters) {
                int allocateCount = newSizeInClusters - oldSizeInClusters;
                if (allocateCount > freeClusterCount)
                    throw new IOException("Disk full.");
                allocateClusters(getShift(startCluster, oldSizeInClusters - 1), allocateCount);
            }
        }
    }

    /**
     * Writes to [fileChannel] along the chain.
     *
     * [startCluster] and [pos] passed by reference for hinting.
     *
     * @param startCluster the head of chain
     * @param pos the byte offset in chain
     * @param src the source of bytes
     * @return the number of bytes that were written
     */
    int writeChannel(Integer startCluster, Long pos, ByteBuffer src) throws IOException {
        int wasWritten;
        synchronized (lockFAT) {
            checkCanWrite();
            int nextToPos = (int)(pos/clusterSize);
            startCluster = getShift(startCluster, nextToPos);
            pos -= nextToPos*clusterSize;

            long startPos = dataOffset + startCluster*clusterSize + pos;

            synchronized (lockData) {
                int limit = src.limit();
                long sizeToWrite = limit - src.position();
                int restOfCluster = (int)(clusterSize - pos);
                if (restOfCluster > limit) {
                    wasWritten = fileChannel.write(src, startPos);
                } else {
                    src.limit(restOfCluster);
                    wasWritten = fileChannel.write(src, startPos);
                    src.limit(limit);
                }
            }
        }
        return wasWritten;
    }

    /**
     * Reads from [fileChannel] along the chain.
     *
     * [startCluster] and [pos] passed by reference for hinting.
     *
     * @param startCluster the head of chain
     * @param pos the byte offset in chain
     * @param dst the destination of bytes
     * @return the number of bytes that were read
     * @throws IOException
     */
    public int readChannel(Integer startCluster, Long pos, ByteBuffer dst) throws IOException {
        int wasRead;
        synchronized (lockFAT) {
            checkCanRead();
            int nextToPos = (int)(pos/clusterSize);
            startCluster = getShift(startCluster, nextToPos);
            pos -= nextToPos*clusterSize;

            long startPos = dataOffset + startCluster*clusterSize + pos;

            synchronized (lockData) {
                int limit = dst.limit();
                long sizeToRead = limit - dst.position();
                int restOfCluster = (int)(clusterSize - pos);
                if (restOfCluster > limit) {
                    wasRead = fileChannel.read(dst, startPos);
                } else {
                    dst.limit(restOfCluster);
                    wasRead = fileChannel.read(dst, startPos);
                    dst.limit(limit);
                }
            }
        }
        return wasRead;
    }

    /**
     * Log the problem to error stream.
     * @param errorMessage  the problem description.
     */
    void LogError(String errorMessage) {
        if (isNormalMode())
            System.err.println(errorMessage);
    }

    private FATSystem(boolean _normalMode) {
        normalMode = _normalMode;
    }

    private void checkCanRead() throws IOException {
        if (!isOpen())
            throw new IOException("The storage is closed.");
    }

    private void checkCanWrite() throws IOException {
        checkCanRead();

        // check for dirty FAT
        if (freeClusterCount < 0 && isNormalMode())
            throw new IOException("The storage needs maintenance.");
    }

    void setDirtyStatus(String message) throws IOException {
        if (freeClusterCount >= 0) {
            freeClusterCount = -1;
            LogError(message);
            checkCanWrite();
        }
    }

    private void initDenormalized() {
        entryPerCluster = clusterSize/FATFile.RECORD_SIZE;
        fatOffset = HEADER_SIZE; //version dependant
        dataOffset = fatOffset + clusterCount*FAT_E_SIZE;
    }

    ByteBuffer allocateBuffer(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(byteOrder);
    }

    private void writeToChannel(ByteBuffer bf) throws IOException {
        while(bf.hasRemaining()) {
            fileChannel.write(bf);
        }
    }

    private void readFromChannel(ByteBuffer bf) throws IOException {
        bf.position(0);
        int size = bf.capacity();
        while(size > 0) {
            size -= fileChannel.read(bf);
        }
        bf.flip();
    }

    /**
     * Reads the entry from FAT by index.
     *
     * @param index the entry index in FAT, not offset!
     * @return entry value
     */
    int getFatEntry(int index) {
        return fatZone.getInt(fatOffset + index*FAT_E_SIZE);
    }

    /**
     * Writes the entry to FAT by index.
     *
     * @param index the entry index in FAT, not offset!
     * @param value to store
     */
    void putFatEntry(int index, int value) {
        fatZone.putInt(fatOffset + index*FAT_E_SIZE, value);
    }

    private FATClusterAllocator createAllocator(int allocatorType) throws IOException {
        switch (allocatorType) {
        case ALLOCATOR_FAST_FORWARD:
            return new FATForwardOnlyClusterAllocator(this);
        case ALLOCATOR_CLASSIC_HEAP:
            return new FATFreeListClusterAllocator(this);
        }
        throw new IOException("Unknown cluster allocator.");
    }

    private static long getRequestedStorageFileSize(int clusterSize, int clusterCount) throws IOException {
        long mapLength = (long)clusterCount*FAT_E_SIZE + HEADER_SIZE;
        if (clusterCount <= 0 || clusterCount > FATClusterAllocator.CLUSTER_INDEX || mapLength > MAPFILE_SIZE_LIMIT)
            throw new IOException("Bad value of cluster count:" + clusterCount);

        long length = (long)clusterCount * clusterSize;
        if (length/clusterSize != clusterCount)
            throw new IOException("File system is too big. FAT overloaded.");

        // max storage size for 4k cluster: CLUSTER_INDEX*4096 = 3FF FFFF F000
        // 0x3FFFFFFF000/0x10000000000 = 3T - big enough.
        long sizeFS = length + mapLength;
        if (sizeFS < length)
            throw new IOException("File system is too big. No space for header." );
        return sizeFS;
    }

    public int getClusterSize() {
        return clusterSize;
    }

}
