package com.test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
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

    private HashMap<Integer, WeakReference<FATFolder>> folderCache = new HashMap<>();
    private HashMap<Integer, WeakReference<FATFile>>   fileCache = new HashMap<>();

    // smart termination procedure as
    //  Transaction counting + shutdown signal + wait for execution finish
    final Object shutdownSignal = new Object();
    private long transactionCounter = 0L;
    private FATFile root;

    private FATFileSystem() {}

    /**
     * Creates new file-based file system.
     *
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
     *
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
            ret.root = FATFolder.ts_createRoot(ret, 0).fatFile;
            success = true;
        } finally {
            if (!success)
                ret.close();
        }
        return ret;
    }


    /**
     * Opens FS from the exist file in host FS.
     *
     * @param path the path to storage file in host FS
     * @return In-file FS over the file that opened in host FS.
     */
    public static FATFileSystem open(Path path) throws IOException {
        return open(path, true);
    }

    /**
     * Opens FS from the exist file in host FS.
     *
     * @param path the path to storage file in host FS
     * @param normalMode the [false] value allows to open dirty FAT for maintenance
     * @return In-file FS over the file that opened in host FS.
     */
    private static FATFileSystem open(Path path, boolean normalMode) throws IOException {
        FATFileSystem ret = new FATFileSystem();
        boolean success = false;
        try {
            ret.fat = FATSystem.open(path, normalMode);
            ret.root = FATFolder.ts_openRoot(ret).fatFile;
            success = true;
        } finally {
            if (!success)
                ret.close();
        }
        return ret;
    }

    /**
     * Closes File System and releases any system resources associated
     * with it.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (fat != null) {
                if (!shutdownRequest())
                    throw new IOException("System was no unmounted.");
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
     * @return the size of storage. That is the [Data Section] size.
     */
    public long getSize() {
        return  fat.getSize();
    }

    /**
     * Returns the free space of File System
     *
     * @return the free size in storage. The [&lt;0] means dirty FAT and the
     *         system needs in maintenance.
     */
    public long getFreeSize() {
        return fat.getFreeSize();
    }

    /**
     * Returns FS time counter.
     *
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
     * @param updateMetadata if [true] - update access info like [lastModified]
     * @throws IOException
     */
    void ts_forceFileContent(FATFile file, boolean updateMetadata) throws IOException {
        fat.forceChannel(updateMetadata);
    }


    void setFileLength(FATFile file, long newLength, long oldLength) throws IOException {
        if (file.ts_getFileId() == FATFile.INVALID_FILE_ID)
            throw new IOException("Bad file state.", file.killer);
        fat.adjustClusterChain(file.ts_getFileId(), newLength, oldLength);
    }

    int writeFileContext(FATFile file, long position,
                                ByteBuffer src) throws IOException {
        int wasWritten = 0;
        
        //pass parameters by reference
        int[] startCluster = new int[]{file.ts_getFileId()};
        long[] pos = new long[]{position};
        while (src.hasRemaining()) {
            int written = fat.writeChannel(startCluster, pos, src);
            if (written == 0)
                break; //chanel is full (transport?)
            wasWritten += written;
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
     * @param parentId parent folder id
     * @param fileName the name of created file
     * @param type FATFile.TYPE_XXXX const
     * @param size the space for allocation
     * @param access desired access to file
     * @return created file
     * @throws IOException
     */
    FATFile ts_createFile(int parentId, String fileName, int type, long size, int access) throws IOException {
        synchronized (this) {
            // create new
            FATFile ret = new FATFile(this, fileName, type, size, access);
            fileCache.put(ret.ts_getFileId(), new WeakReference<>(ret));
            if (!ret.isRoot())
                ret.ts_setParent(ts_getFolder(parentId));
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
     *
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
            if (type == FATFile.TYPE_DELETED)
               return FATFile.DELETED_FILE;

            WeakReference<FATFile> r = fileCache.get(fileId);
            FATFile ret = (r == null) ? null : r.get();
            if (ret == null) {
                // open existent
                fat.checkFileId(fileId); //check & mark dirty
                ret = new FATFile(this, type, fileId);
                ret.ts_initFromBuffer(bf);
                fileCache.put(ret.ts_getFileId(), new WeakReference<>(ret));
                if (!ret.isRoot())
                    ret.ts_setParent(ts_getFolder(parentId));
            } else
                ret.ts_initFromBuffer(bf);
            return ret;
        }
    }

    /**
     * Restores folder from the file unique Id or gets it from cache.
     *
     * @param fileId the file unique Id in File System.
     * @return the folder object
     */
    FATFolder ts_getFolder(int fileId) throws IOException {
        synchronized (this) {
            WeakReference<FATFolder> r = folderCache.get(fileId);
            FATFolder ret = (r == null) ? null : r.get();
            if (ret == null) {
                //ts_ constructor
                ret = new FATFolder(ts_getFile(fileId));
                folderCache.put(fileId, new WeakReference<>(ret));
                ret.ts_readContent();
            }
            return ret;
        }
    }

    /**
     * Gets a file object from cache, or return null.
     *
     * @param fileId  cached FATFile if any.
     * @return
     */
    FATFile ts_getFile(int fileId) throws IOException {
        synchronized (this) {
            WeakReference<FATFile> r = fileCache.get(fileId);
            FATFile ret = (r == null) ? null : r.get();            
            if (ret == null) {
                fat.setDirtyState("Cache damaged", true);
            }
            return ret;
        }
    }

    public FATFolder getRoot() throws IOException {
        //return FATFolder.ts_getRoot(this);
        return root.getFolder();
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
    void ts_setDirtyState(String message, boolean throwException) throws IOException {
        fat.setDirtyState(message, throwException);
    }

    void updateRootRecord(ByteBuffer rootInfo) throws IOException {
        fat.writeRootInfo(rootInfo);
    }

    public ByteBuffer getRootInfo() throws IOException {
        return fat.getRootInfo();
    }

    void disposeFile(int fileId) {
        synchronized (this) {
            fileCache.remove(fileId);
        }
    }
    void disposeFolder(int folderId) {
        synchronized (this) {
            folderCache.remove(folderId);
        }
    }

    //{debug-test
    int getFileCacheSize() {
        synchronized (this) {
            return fileCache.size();
        }
    }
    
    int getFolderCacheSize() {
        synchronized (this) {
            return folderCache.size();
        }
    }

    void force() throws IOException {
        fat.force();
    }
    //}debug-test        
}
