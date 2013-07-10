package com.test;

import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;


/**
 * Storage object for unmarked chain.
 *
 * Realized over the FAT table.
 *
 * Max storage size for 4k cluster: CLUSTER_INDEX*4096 = 3FF FFFF F000
 * 0x3FFFFFFF000/0x10000000000 = 3T - big enough.
 */


class FATSystem implements Closeable {
    final static int ALLOCATOR_CLASSIC_HEAP = 0;
    final static int ALLOCATOR_FAST_FORWARD = 1;

    // file system header with magic number abd etc
    final static int  HEADER_HEAD_SIZE_RESERVED = 28;
    // FAT Allocator header space just before FAT
    final static int  HEADER_TAIL_SIZE_RESERVED = 4;
    final static int  HEADER_SIZE = HEADER_HEAD_SIZE_RESERVED
                                  + FATFile.RECORD_SIZE
                                  + HEADER_TAIL_SIZE_RESERVED;
    final static int  FREE_CLUSTER_COUNT_OFFSET = 5*4;
    final static int  ROOT_RECORD_OFFSET = HEADER_HEAD_SIZE_RESERVED;
    final static int  VERSION     = 1;
    final static long MAPFILE_SIZE_LIMIT = Integer.MAX_VALUE;

    final static int FAT_E_SIZE  = 4; //bytes for FAT32
    final static int MAGIC_WORD  = 0x75616673;
    //final static int MAGIC_WORD  = 0x73666175; //check ENDIAN sfau/uafs as BIG/LITTLE

    //header
    private int fsVersion;
    private int clusterSize;
    int clusterCount;

    //offsets in file
    private int fatOffset;
    private int dataOffset;

    //the number of free clusters in system
    int freeClusterCount; //can get [-1] on "dirty FAT"

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer fatZone;
    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN; //default encoding (currently fixed)
    private FATClusterAllocator clusterAllocator;

    private final boolean normalMode;

    void checkFileId(int fileId) throws IOException {
        int fatEntry = getFatEntry(fileId);
        if ((fatEntry & FATClusterAllocator.CLUSTER_ALLOCATED) == 0)
            setDirtyState("Invalid file id.", true);
    }

    /**
     * state machine
     */
    static enum SystemState {
        INIT,
        ACTIVE,
        DIRTY,
        SHUTDOWN_REQUEST,
        SHUTDOWN,
        CLOSED
    }

    SystemState state = SystemState.INIT;


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
        if (freeClusterCount < 0) {
            state = SystemState.DIRTY;
            LogError("Open for read-only. Dirty state.");
        } else {
            state = SystemState.ACTIVE;
        }

        // max storage size for 4k cluster: CLUSTER_INDEX*4096 = 3FF FFFF F000
        // 0x3FFFFFFF000/0x10000000000 = 3T - big enough.
        long sizeFS = getRequestedStorageFileSize(clusterSize, clusterCount);

        if (randomAccessFile.length() < sizeFS) {
            setDirtyState("Wrong storage size. Storage was truncated in host FS.", true);
        }
        
        initDenormalized();
        // map FAT section
        fatZone = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fatOffset + clusterCount*FAT_E_SIZE);
        clusterAllocator = createAllocator(allocatorType);
        clusterAllocator.initFromFile();
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
            ret.state = SystemState.ACTIVE;
        } finally {
            if (!success) {
                //rest in [INIT] state - no action
                ret.close();
            }        
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
        //writeToChannel( allocateBuffer(HEADER_SIZE)
            // init header
            .putInt(MAGIC_WORD)
            .putInt(fsVersion)     //FS version
            .putInt(allocatorType)
            .putInt(clusterSize)
            .putInt(clusterCount)
            //Set dirty flag in free cluster count. We drop it on right close.
            .putInt(-1);
        //);
        clusterAllocator = createAllocator(allocatorType);
        clusterAllocator.initFAT();
        forceFat();
    }

    private void writeFreeClusterCount(int value) {
        synchronized (this) {
            //fully constructed FS
            if (isNormalMode() 
                    && (state != SystemState.CLOSED) 
                    && (fatZone != null)) 
            {
                // saves real dirty status
                fatZone.putInt(FREE_CLUSTER_COUNT_OFFSET, value);
            }
        } 
    }

    void writeRootInfo(ByteBuffer rootInfo) throws IOException {
        if (rootInfo.remaining() > FATFile.RECORD_SIZE)
            throw new IOException("Wrong root info.");
        synchronized (this) {
            checkCanWrite();
            fatZone.position(ROOT_RECORD_OFFSET);
            fatZone.put(rootInfo);
        }
    }

    ByteBuffer getRootInfo() throws IOException {
        byte[] bb = new byte[FATFile.RECORD_SIZE];
        synchronized (this) {
            checkCanRead();
            fatZone.position(ROOT_RECORD_OFFSET);
            fatZone.get(bb);
            return ByteBuffer.wrap(bb).order(byteOrder);
        }
    }

    void markDiskStateDirty() {
        writeFreeClusterCount(-1);
    }

    void markDiskStateActual() {
        writeFreeClusterCount(freeClusterCount);
    }


    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (state == SystemState.CLOSED)
                throw new IOException("Storage was closed earlier.");
            
            boolean needGCrun = false;
            try {
                if (randomAccessFile != null) {
                    if (fileChannel != null) {
                        if (fatZone != null) {
                            markDiskStateActual();
                            force();
                            if (fatZone instanceof DirectBuffer) {
                                // That is bad, but it is the only available solution
                                // http://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java
                                sun.misc.Cleaner cleaner = ((DirectBuffer)fatZone).cleaner();
                                // critical point: fatZone have to be [null] ASAP to avoid 
                                // finalizer call with double free and JVM crash
                                fatZone = null;
                                cleaner.clean();
                            } else {
                                fatZone = null;
                                LogError("Not Oracle implementation for memory-mapped file."
                                       + "We can get a problem. Trying direct GC call.");
                            }
                            needGCrun = true;
                        }
                        fileChannel.close();
                    }
                    randomAccessFile.close();
                }
            } finally {
                // RIP
                state = SystemState.CLOSED;
                if (needGCrun)
                    System.gc();
            }
        }
    }

    public void forceChannel(boolean updateMetadata) throws IOException {
        boolean success = false;
        try {
            fileChannel.force(updateMetadata);
            success = true;
        } finally {
            if (!success)
                setDirtyState("Cannot force data to host FS.", false);
        }
    }

    public void force() throws IOException {
        synchronized (this) {
            // One is not a guaranty for another            
            forceChannel(true);
            forceFat();
        }
    }

    /**
     * Flush content to disk.
     * Have to be called in synchronized section
     */
    private void forceFat() throws IOException {
        clusterAllocator.force();
        fatZone.force();
    }


    int getVersion() {
       return fsVersion;
    }

    /**
     * Returns the capacity of FS
     * @return the size of storage. That is the [Data Section] size.
     */
    public long getSize() {
        return  clusterCount*clusterSize;
    }

    /**
     * Returns the cluster size of FS
     * @return the size of cluster.
     */
    public int getClusterSize() {
        return clusterSize;
    }

    /**
     * Returns the free capacity of FS
     * @return the free size in storage. The [<0] means dirty FAT and the system needs in maintenance.
     */
    public long getFreeSize() {
        return freeClusterCount*clusterSize;
    }

    /**
     * Reads cluster content. Need for maintenance.
     *
     * @param cluster the index of the cluster in FAT
     * @return cluster content
     * @throws IOException
     */
    public ByteBuffer readCluster(int cluster) throws IOException {
        if (cluster < 0 || cluster >= clusterCount)
            throw new IOException("Bad cluster index:" + cluster);

        ByteBuffer bf = ByteBuffer.allocateDirect(clusterSize);  //check with alloc!
        synchronized (this) {
            checkCanRead();
            fileChannel
                .position(dataOffset + cluster * clusterSize)
                .read(bf);
        }
        return bf;
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
        synchronized (this) {
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
                    //forceFat();
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
        synchronized (this) {
            checkCanWrite();
            try {
                clusterAllocator.freeClusters(headOffset, freeHead);
            } finally {
                //forceFat();
            }
        }
    }

    /**
     * Calculates the length of chain in clusters to hold [size] bytes.
     *
     * @param size the number of bytes to hold
     * @return the length of the chain
     */
    public int getSizeInClusters(long size) {
        return (int)getSizeInUnits(size, clusterSize);
    }

    /**
     * Finds the [newSizeInClusters] value in the list that starts from [startCluster]
     *
     * Have to be called in synchronized section.
     *
     * @param startCluster the start of the chain
     * @param nextCount the number of [next] actions in list.
     * @return the index of cluster that is at [newSizeInClusters] pos in chain.
     */
    private int getShift(int startCluster, int nextCount) throws IOException {
        while (nextCount > 0) {
            int fatEntry = getFatEntry(startCluster);
            // CLUSTER_ALLOCATED only
            if ((fatEntry & FATClusterAllocator.CLUSTER_ALLOCATED) == FATClusterAllocator.CLUSTER_ALLOCATED) {
                startCluster = fatEntry & FATClusterAllocator.CLUSTER_INDEX;
            } else {
                setDirtyState("Cluster chain is broken. Cluster#:" + startCluster
                        + " Value:" + fatEntry, true);
            }
            --nextCount;
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
        synchronized (this) {
            checkCanWrite();
            // check only public parameters
            if (newLength < 0 || newLength > getSize())
                throw new IOException("Wrong FATFile size:" + newLength);
            int oldSizeInClusters = (int)getSizeInUnits(oldLength, clusterSize);
            int newSizeInClusters = (int)getSizeInUnits(newLength, clusterSize);
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
    int writeChannel(int[] startCluster, long[] pos, ByteBuffer src) throws IOException {
        int wasWritten;
        synchronized (this) {
            checkCanWrite();
            int nextToPos = (int)(pos[0]/clusterSize);
            startCluster[0] = getShift(startCluster[0], nextToPos);
            pos[0] -= nextToPos*clusterSize;

            long startPos = dataOffset + startCluster[0]*clusterSize + pos[0];

            int limit = src.limit();
            int restOfCluster = (int)(clusterSize - pos[0]);
            if (restOfCluster >= src.remaining()) {
                wasWritten = fileChannel.write(src, startPos);
            } else {
                src.limit(src.position() + restOfCluster);
                wasWritten = fileChannel.write(src, startPos);
                src.limit(limit);
            }
            pos[0] += wasWritten;
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
    public int readChannel(int[] startCluster, long[] pos, ByteBuffer dst) throws IOException {
        int wasRead;
        synchronized (this) {
            checkCanRead();
            int nextToPos = (int)(pos[0]/clusterSize);
            startCluster[0] = getShift(startCluster[0], nextToPos);
            pos[0] -= nextToPos*clusterSize;

            long startPos = dataOffset + startCluster[0]*clusterSize + pos[0];

            int limit = dst.limit();
            int restOfCluster = (int)(clusterSize - pos[0]);
            if (restOfCluster > dst.remaining()) {
                wasRead = fileChannel.read(dst, startPos);
            } else {
                dst.limit(dst.position() + restOfCluster);
                wasRead = fileChannel.read(dst, startPos);
                dst.limit(limit);
            }
            pos[0] += wasRead;
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

    void checkCanRead() throws IOException {
        if (state == SystemState.INIT)        
            throw new IOException("The storage was not initialized.");
        if (state == SystemState.CLOSED)        
            throw new IOException("The storage is closed.");
    }

    void checkCanWrite() throws IOException {
        checkCanRead();
        // check for dirty FAT
        if ((state == SystemState.DIRTY) && isNormalMode())
            throw new IOException("The storage needs maintenance.");
    }

     /**
     * Mark storage as [dirty]
     *
     * @param message the message to log.
     * @param throwException [true] if we need notify about the problem by exception throw
     * @throws IOException
     */
    void setDirtyState(String message, boolean throwException) throws IOException {
        if (state != SystemState.DIRTY) {
            state = SystemState.DIRTY;
            freeClusterCount = -1;
            LogError(message);
            if (throwException)
                checkCanWrite();
        }
    }

    private void initDenormalized() {
        fatOffset = HEADER_SIZE; //version dependant
        dataOffset = (int)getDataOffset(fatOffset + clusterCount*FAT_E_SIZE);
    }

    ByteBuffer allocateBuffer(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(byteOrder);
        //return ByteBuffer.allocate(capacity).order(byteOrder);
    }

    private void writeToChannel(ByteBuffer bf, long position) throws IOException {
        synchronized (this) {
            checkCanWrite();
            fileChannel.position(position);
            while(bf.hasRemaining()) {
                fileChannel.write(bf);
            }
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
        /* Uncomment the alternative procedure to compare the performance
        ByteBuffer bf = allocateBuffer(4);
        fileChannel.position(fatOffset + index * FAT_E_SIZE);
        fileChannel.read(bf);
        bf.flip();
        return bf.getInt();
        */
    }

    /**
     * Writes the entry to FAT by index.
     *
     * @param index the entry index in FAT, not offset!
     * @param value to store
     */
    void putFatEntry(int index, int value) {
        fatZone.putInt(fatOffset + index*FAT_E_SIZE, value);
        /* Uncomment the alternative procedure to compare the performance
        ByteBuffer bf = allocateBuffer(4);
        bf.putInt(value).flip();
        fileChannel.write(bf, fatOffset + index * FAT_E_SIZE);
        */
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

    /**
     * Calculates the length of chain in units to hold [size] bytes.
     *
     * @param size the number of bytes to hold
     * @param unitSize the unit capacity in bytes
     * @return the length of the chain
     */
    public static long getSizeInUnits(long size, long unitSize) {
        if (size < unitSize)
            return 1;
        int fullClusters = (int)(size/unitSize);
        return (fullClusters*unitSize == size)
                ? fullClusters
                : fullClusters + 1;
    }

    private static long getDataOffset(long mapSize) {
        //PERFORMANCE HINT POINT - 4k alignment for memory mapping.
        //return getSizeInUnits(mapSize, 4096) * 4096; //4k alignment
        return mapSize; // size first
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
        long sizeFS = length + getDataOffset(mapLength);
        if (sizeFS < length)
            throw new IOException("File system is too big. No space for header." );
        return sizeFS;
    }
}
