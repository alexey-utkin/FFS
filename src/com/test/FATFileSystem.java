package com.test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 *
 * The File System over the FAT System.
 *
 * All [transact-safe] methods have the [ts_] prefix.
 * The [transact-safe] means that methods can terminated successfully,
 *   or restore the object state to initial condition and throw exception,
 *   or mark system as "dirty" and throw exception (critical error in host FS).
 *
 * All public functions have to be [transact-safe] by default.
 *
 * Each public call with r/w operation need to be executed in FS transaction like
 * <pre><code>
 *              boolean success = false;
 *              begin(isWriteTransaction);
 *              try {
 *                  ...action...
 *                  //commit
 *                  success = true;
 *             } finally {
 *                 if (!success) {
 *                     //rollback action
 *                 }
 *                 end();
 *             }
 * </code></pre>
 */
public class FATFileSystem implements Closeable {
    private FATSystem fat;
    private FATFolder root;

    private HashMap<Integer, FATFolder> folderCache = new HashMap<>();
    private HashMap<Integer, FATFile>   fileCache = new HashMap<>();

    // smart termination procedure as
    //  Transaction counting + shutdown signal + wait for execution finish
    final Object shutdownSignal = new Object();
    private long transactionCounter = 0L;

    private FATFileSystem() {}

    /**
     * Creates new file-based file system.
     * @param path is the path in host FS for file storage that need be created
     * @param clusterSize  the size of single cluster. Mast be at least [FATFile.RECORD_SIZE] size
     * @param clusterCount the total number of clusters in created file storage.
     * @return new In-file FS over the file that created in host FS.
     * @throws IOException for bad parameters or file access problem in the host FS
     */
    public static FATFileSystem create(Path path, int clusterSize, int clusterCount) throws IOException {
        return create(path, clusterSize, clusterCount, FATSystem.ALLOCATOR_CLASSIC_HEAP);
    }

    /**
     * Creates new file-based file system.
     * @param path is the path in host FS for file storage that need be created
     * @param clusterSize  the size of single cluster. Mast be at least [FATFile.RECORD_SIZE] size
     * @param clusterCount the total number of clusters in created file storage.
     * @param allocatorType the cluster allocation strategy
     * @return new In-file FS over the file that created in host FS.
     * @throws IOException for bad parameters or file access problem in the host FS
     */
    public static FATFileSystem create(Path path, int clusterSize,int clusterCount,
                                   int allocatorType) throws IOException {
        FATFileSystem ret = new FATFileSystem();
        boolean success = false;
        try {
            ret.fat = FATSystem.create(path, clusterSize, clusterCount, allocatorType);
            ret.root = FATFolder.ts_createRoot(ret, 0);
            success = true;
        } finally {
            if (!success)
                ret.close();
        }
        return ret;
    }


    /**
     * Opens FS from the exist file in host FS.
     * @param path the path to storage file in host FS
     * @return In-file FS over the file that opened in host FS.
     */
    public static FATFileSystem open(Path path) throws IOException {
        return open(path, true);
    }

    /**
     * Opens FS from the exist file in host FS.
     * @param path the path to storage file in host FS
     * @param normalMode the [false] value allows to open dirty FAT for maintenance
     * @return In-file FS over the file that opened in host FS.
     */
    private static FATFileSystem open(Path path, boolean normalMode) throws IOException {
        FATFileSystem ret = new FATFileSystem();
        boolean success = false;
        try {
            ret.fat = FATSystem.open(path, normalMode);
            ret.root = FATFolder.ts_openRoot(ret);
            success = true;
        } finally {
            if (!success)
                ret.close();
        }
        return ret;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     * 
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (!shutdownRequest())
                throw new IOException("System was no unmounted.");

            if (fat != null) {
                // here only [clean] close can happen
                // it saves actual value of [dirty] status
                fat.close();
            }
        }
    }

    /**
     * Get file system version.
     *
     * @return version of file system in action.
     */
    public int getVersion() {
        // single version for all system
        return fat.getVersion();
    }

    /**
     * Returns the capacity of File System.
     *
     * No staff info above pure FAT.
     *
     * @return the size of storage. That is the [Data Section] size.
     */
    public long getSize() {
        return  fat.getSize();
    }

    /**
     * Returns the free capacity of File System
     *
     * No forward reservation for staff objects.
     *
     * @return the free size in storage. The [&lt;0] means dirty FAT and the
     *         system needs in maintenance.
     */
    public long getFreeSize() {
        return fat.getFreeSize();
    }

    /**
     * Returns FS time counter.
     * @return the Java current time in milliseconds.
     */
    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }

    ByteBuffer ts_allocateBuffer(int recordSize) {
        // potentially the Folder record could be in reverse byte order,
        // but it is not a good idea
        return fat.allocateBuffer(recordSize);
    }

    int ts_allocateFileSpace(long size) throws IOException {
        if (size < 0)
            throw new IOException("Wrong file size.");

        // use startCluster as fileId
        return fat.allocateClusters(-1, fat.getSizeInClusters(size));
    }

    /**
     * Flush file content.
     *
     * @param file the file to flush
     * @param updateMetadata
     * @throws IOException
     */
    void ts_forceFileContent(FATFile file, boolean updateMetadata) throws IOException {
        fat.forceChannel(updateMetadata);
    }


    void setFileLength(FATFile file, long newLength, long oldLength) throws IOException {
        if (file.ts_getFileId() == FATFile.INVALID_FILE_ID) {
            file.ttt.printStackTrace();
            throw new IOException("Bad file state.");
        }
        fat.adjustClusterChain(file.ts_getFileId(), newLength, oldLength);
    }

    int writeFileContext(FATFile file, long position,
                                ByteBuffer src) throws IOException {
        int wasWritten = 0;
        
        //pass parameters by reference
        int[] startCluster = new int[]{file.ts_getFileId()};
        long[] pos = new long[]{position};
        while (src.hasRemaining()) {
            wasWritten += fat.writeChannel(startCluster, pos, src);
        }
        return wasWritten;
    }

    int readFileContext(FATFile file, long position,
                        ByteBuffer dst) throws IOException {
        int wasRead = 0;
        int[] startCluster = new int[]{file.ts_getFileId()};
        long[] pos = new long[]{position};
        while (dst.hasRemaining()) {
            int read = fat.readChannel(startCluster, pos, dst);
            if (read < 0) {
                if (wasRead == 0)
                    return -1;
                break;
            }
            wasRead += read;
        }
        return wasRead;
    }

    /**
     * Creates new file.
     *
     * Returns file has no connection with folder.
     *
     * @param fileName the name of created file
     * @param type FATFile.TYPE_XXXX const
     * @param size the space for allocation
     * @param access desired access to file
     * @return created file
     * @throws IOException
     */
    FATFile ts_createFile(String fileName, int type, long size, int access) throws IOException {
        synchronized (this) {
            // create new
            FATFile ret = new FATFile(this, fileName, type, size, access);
            fileCache.put(ret.ts_getFileId(), ret);
            return ret;
        }
    }

    /**
     * Rollback procedure for [{@see ts_createFile}] return value
     *
     * @param file the file for drop.
     */
    void ts_dropDirtyFile(FATFile file) throws IOException {
        synchronized (this) {
            if (file.ts_getFileId() == FATFile.INVALID_FILE_ID)
                throw new IOException("Bad file id.");
            
            fileCache.remove(file.ts_getFileId());
            try {
                fat.freeClusters(file.ts_getFileId(), true);
            } finally {
                //no rollback from fat level - set dirty inside
                file.ts_setFileId(FATFile.INVALID_FILE_ID);
            }
        }
    }

    /**
     * Opens file from folder record.
     * Returns file that has no connection with folder.
     *
     * @param bf the storage of attributes
     * @return opened file
     * @throws IOException
     */
    FATFile ts_openFile(ByteBuffer bf, int parentId) throws IOException {
        synchronized (this) {
            // open existent if can
            int fileId = bf.getInt();
            int type = bf.getInt();
            FATFile ret =  fileCache.get(fileId);
            if (ret == null) {
                // open existent
                int fatEntry = fat.getFatEntry(fileId);
                if ((fatEntry & FATClusterAllocator.CLUSTER_ALLOCATED) == 0)
                    throw new IOException("Invalid file id.");

                ret = new FATFile(this, type, fileId, parentId);
                fileCache.put(ret.ts_getFileId(), ret);
            } // else check the type?
            ret.ts_initFromBuffer(bf);
            return ret;
        }
    }

    /**
     * Restores folder from [fileId] or gets it from cache.
     *
     * @param fileId the [fileId] of folder storage file.
     * @return the folder object
     */
    FATFolder ts_getFolder(int fileId) {
        synchronized (this) {
            FATFolder ret = folderCache.get(fileId);
            if (ret == null) {
                ret = new FATFolder(ts_getFile(fileId));
                folderCache.put(fileId, ret);
            }
            return ret;
        }
    }

    /**
     * Gets a file object from cache, or return null.
     *
     * @param fileId
     * @return
     */
    FATFile ts_getFile(int fileId) {
        synchronized (this) {
            return  fileCache.get(fileId);
        }
    }

    public FATFolder getRoot() {
        return root;
    }

    /**
     * Signal to start transaction.
     */
    void begin(boolean writeOperation) throws IOException {
        synchronized (this) {
            if (fat.state.ordinal() >= FATSystem.SystemState.SHUTDOWN.ordinal())
                throw new IOException("System down.");   
            
            if (transactionCounter == 0) {
                // we start concurrent transaction pull,
                // mark state as [dirty] in external memory. 
                // Right nested stack restores 
                // actual state in paired [end] calls.
                fat.markDiskStateDirty();
            }
            if (writeOperation)
                fat.checkCanWrite();
            else
                fat.checkCanRead();

            // we need unwind nested transactions.
            // [end] will called in any case!
            transactionCounter += 1;
        }
    }

    
    private void checkEmptyTransactionPool() {
        if (transactionCounter == 0) {
            // All transactions are finished.
            // Mark state of FS in external memory by result
            fat.markDiskStateActual();
            if (fat.state == FATSystem.SystemState.SHUTDOWN_REQUEST) {
                fat.state = FATSystem.SystemState.SHUTDOWN;
                synchronized (shutdownSignal) {
                    shutdownSignal.notify();
                }
            }
        }
    }
    
    /**
     * Signal to end transaction.
     */
    void end() {
        synchronized (this) {
            transactionCounter -= 1;
            checkEmptyTransactionPool();
        }
    }

    /**
     * Sends shutdown signal to the file system.
     *
     * Can be called multiple times. Once all nested
     * transactions are terminated, the file system goes to
     * the frozen state and ready to be closed.
     *
     * If there are no active transactions in the thread stack,
     * the [{@link #waitForShutdown()}] method could be called directly.
     *
     * @return [true] if the file system is ready be closed
     */
    public boolean shutdownRequest() {
        synchronized (this) {
            if (fat.state.ordinal() < FATSystem.SystemState.SHUTDOWN_REQUEST.ordinal()) {
                fat.state = FATSystem.SystemState.SHUTDOWN_REQUEST;
            }
            checkEmptyTransactionPool();
            return (fat.state == FATSystem.SystemState.SHUTDOWN);
        }
    }

    /**
     * Waits for the file system shutdown.
     *
     * @throws InterruptedException
     * @see    java.lang.Object#wait() ()
     */
    public void waitForShutdown() throws InterruptedException {
        if (!shutdownRequest()) {
            synchronized (shutdownSignal) {
                shutdownSignal.wait();
            }
        }
    }

    /**
     * Set system to dirty state.
     *
     * @param message
     */
    void ts_setDirtyState(String message, boolean throwExeption) throws IOException {
        fat.setDirtyState(message, throwExeption);
    }

    void updateRootRecord(ByteBuffer rootInfo) throws IOException {
        fat.writeRootInfo(rootInfo);
    }

    public ByteBuffer getRootInfo() throws IOException {
        return fat.getRootInfo();
    }
}
