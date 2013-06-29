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
 */
public class FATFileSystem implements Closeable {
    private FATSystem fat;
    private FATFolder root;
    private HashMap<Integer, FATFolder> folderCache = new HashMap<>();
    private HashMap<Integer, FATFile>   fileCache = new HashMap<>();

    // treeLock->fileLock lock sequence
    final Object treeLock = new Object();
    final Object fileLock = new Object();

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
            ret.root = FATFolder.createRoot(ret, 0);
            success = true;
        } finally {
            if (!success)
                ret.close();
        }
        return ret;
    }

    private FATFolder createFolder(String name, FATFolder parent) throws IOException {
        int access = 0;
        //return FATFolder.create(this, name, parent, access);
        return null;
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
            ret.root = FATFolder.openRoot(ret);
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
     * <p/>
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
        if (fat != null)
            fat.close();
    }

    /**
     * Returns FS time counter.
     * @return the Java current time in milliseconds.
     */
    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }

    ByteBuffer allocateBuffer(int recordSize) {
        // potentially the Folder record could be in reverse byte order,
        // but it is not a good idea
        return fat.allocateBuffer(recordSize);
    }

    int getVersion() {
        // single version for all system
        return fat.getVersion();
    }

    int allocateFileSpace(long size) throws IOException {
        if (size < 0)
            throw new IOException("Wrong file size.");

        // use startCluster as fileId
        return fat.allocateClusters(-1, fat.getSizeInClusters(size));
    }

    void deleteFile(FATFile fatFile) throws IOException {
        if (fatFile.fileId == FATFile.INVALID_FILE_ID)
            throw new IOException("Bad file state.");
        try {
            fat.freeClusters(fatFile.fileId, true);
        } finally {
            fatFile.fileId = FATFile.INVALID_FILE_ID;
        }
    }

    void forceFileContent(FATFile fatFile, boolean updateMetadata) throws IOException {
        fat.forceChannel(updateMetadata);
    }


    void setFileLength(FATFile fatFile, long newLength) throws IOException {
        if (fatFile.fileId == FATFile.INVALID_FILE_ID)
            throw new IOException("Bad file state.");
        fat.adjustClusterChain(fatFile.fileId, newLength, newLength);
    }

    int writeFileContext(FATFile fatFile, long position,
                                ByteBuffer src) throws IOException {
        int wasWritten = 0;
        Integer startCluster = fatFile.fileId;
        Long pos = position;
        while (src.hasRemaining()) {
            wasWritten += fat.writeChannel(startCluster, pos, src);
        }
        return wasWritten;
    }

    int readFileContext(FATFile fatFile, long position,
                        ByteBuffer dst) throws IOException {
        int wasRead = 0;
        Integer startCluster = fatFile.fileId;
        Long pos = position;
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
     * Returns file has no connection with folder.
     *
     * @param type FATFile.TYPE_XXXX const
     * @param size the space for allocation
     * @param access desired access to file
     * @return created file
     * @throws IOException
     */
    FATFile createFile(int type, long size, int access) throws IOException {
        synchronized (fileLock) {
            // create new
            FATFile ret = new FATFile(this, type, size, access);
            fileCache.put(ret.getFileId(), ret);
            return ret;
        }
    }

    /**
     * Opens root folder file or file for maintenance.
     * Returns file has no connection with folder.
     *
     * @param type   FATFile.TYPE_XXXX const
     * @param fileId real file id (the index of chain start)
     * @return opened file
     * @throws IOException
     */
    FATFile openFile(int type, int fileId) throws IOException {
        synchronized (fileLock) {
            // open existent
            int fatEntry = fat.getFatEntry(fileId);
            if ((fatEntry & FATClusterAllocator.CLUSTER_ALLOCATED) == 0)
                throw new IOException("Invalid file id.");

            FATFile ret = new FATFile(this, type, fileId);
            fileCache.put(ret.getFileId(), ret);
            return ret;
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
    FATFile openFile(ByteBuffer bf) throws IOException {
        synchronized (fileLock) {
            // open existent if can
            int fileId = bf.getInt();
            FATFile ret =  fileCache.get(fileId);
            if (ret == null) {
                ret = new FATFile(this, fileId, bf);
                fileCache.put(ret.getFileId(), ret);
            }
            return ret;
        }
    }



    /**
     * Restores folder from [fileId] or gets it from cache.
     *
     * @param fileId
     * @return
     */
    FATFolder getFolder(int fileId) {
        synchronized (treeLock) {
            FATFolder ret = folderCache.get(fileId);
            if (ret == null) {
                ret = new FATFolder(getFile(fileId));
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
    FATFile getFile(int fileId) {
        synchronized (fileLock) {
            return  fileCache.get(fileId);
        }
    }

}
