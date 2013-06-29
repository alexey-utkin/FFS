package com.test;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 *
 * The File System over the FAT System.
 */
public class FATFileSystem implements Closeable {
    FATSystem fat;

    private FATFileSystem() {}

    /**
     * Creates new file-based file system.
     * @param path is the path in host FS for file storage that need be created
     * @param clusterSize  the size of single cluster. Mast be at least [FolderEntry.RECORD_SIZE] size
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
     * @param clusterSize  the size of single cluster. Mast be at least [FolderEntry.RECORD_SIZE] size
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
            FATFolder.createRoot(ret, 0);
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

        // use startCluster as fileID
        return fat.allocateClusters(-1, fat.getSizeInClusters(size));
    }

    void deleteFile(FATFile fatFile) throws IOException {
        if (fatFile.fileID == FATFile.INVALID_FILE_ID)
            throw new IOException("Bad file state.");
        fat.freeClusters(fatFile.fileID, true);
        fatFile.fileID = -1;
    }

    void forceFileContent(FATFile fatFile, boolean updateMetadata) throws IOException {
        fat.forceChannel(updateMetadata);
    }


    void setFileLength(FATFile fatFile, long newLength) throws IOException {
        if (fatFile.fileID == FATFile.INVALID_FILE_ID)
            throw new IOException("Bad file state.");
        fat.adjustClusterChain(fatFile.fileID, newLength, newLength);
    }

    public int writeFileContext(FATFile fatFile, long position,
                                ByteBuffer src) throws IOException {
        int wasWritten = 0;
        Integer startCluster = fatFile.fileID;
        Long pos = position;
        while (src.hasRemaining()) {
            wasWritten += fat.writeChannel(startCluster, pos, src);
        }
        return wasWritten;
    }
}
