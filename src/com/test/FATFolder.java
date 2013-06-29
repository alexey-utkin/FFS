package com.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 */

class FATFolder {
    private static final String ROOT_NAME = "<root>";
    //to check long in call params
    private static final long EMPTY_FILE_SIZE = 0L;
    private static final int  ROOT_FILE_ID = 0;
    final FATFile fatFile;

    //PERFORMANCE HINT POINT
    //use different collections for main operations
    final ArrayList<FATFile> childFiles = new ArrayList<>();


    /**
     * Creates Folder from File.
     *
     * Could not be called directly, use [fs.getFolder] instead.
     *
     * @param _fatFile the folder storage
     */
    FATFolder(FATFile _fatFile) {
        fatFile = _fatFile;
    }


    /**
     * Creates root folder on empty storage.
     *
     * @param fs the File System to mount in.
     * @param access the desired access
     * @return the Root Folder object
     * @throws IOException
     */
    static FATFolder createRoot(FATFileSystem fs, int access) throws IOException {
        // exclusive access to [ret]
        FATFile rootFile = fs.createFile(FATFile.TYPE_FOLDER, EMPTY_FILE_SIZE, access);
        if (rootFile.getFileId() != ROOT_FILE_ID)
            new IOException("Root already exists.");

        rootFile.initName(ROOT_NAME);
        // self store
        FATFolder ret = fs.getFolder(rootFile.getFileId());
        rootFile.moveTo(ret);
        return ret;
    }

    /**
     * Opens existent root folder.
     *
     * @param fs the File System to read from.
     * @return the Root Folder object
     * @throws IOException
     */
    static FATFolder openRoot(FATFileSystem fs) throws IOException {
        FATFile rootFile = fs.openFile(FATFile.TYPE_FOLDER, ROOT_FILE_ID);
        // exclusive access to [ret]
        FATFolder ret = fs.getFolder(rootFile.getFileId());
        ret.readContent();
        if (ret.childFiles.isEmpty()
            || !ROOT_NAME.equals(ret.childFiles.get(0).toString()))
            throw new IOException("Root folder is damaged!");
        return ret;
    }

    private void readContent() throws IOException {
        synchronized (fatFile.getLockContent()) {
            try (FATFileChannel folderContent = fatFile.getChannel(false)) {
                ByteBuffer bf = fatFile.fs.allocateBuffer(FATFile.RECORD_SIZE);
                long folderStorageSize = fatFile.length();
                while (folderContent.position() < folderStorageSize) {
                    folderContent.read(bf);
                    bf.flip();
                    childFiles.add(fatFile.fs.openFile(bf));
                    bf.position(0);
                }
                if (folderContent.position() != folderStorageSize)
                    throw new IOException("Folder is damaged!");
            }
        }
    }

    public static FATFolder create(FATFolder parent, String name, int access) throws IOException {
        return null;
    }

    public int getFolderId() {
        return fatFile.getFileId();
    }

    private void updateFileRecord(int index, FATFile updateFile) throws IOException {
        try (FATFileChannel folderContent = fatFile.getChannel(false)) {
            folderContent
                .position(index * FATFile.RECORD_SIZE)
                .write((ByteBuffer) updateFile
                        .serialize(fatFile.fs.allocateBuffer(FATFile.RECORD_SIZE),
                                fatFile.fs.getVersion())
                        .flip());
        }
    }

    void updateFileRecord(FATFile updateFile) throws IOException {
        synchronized (fatFile.getLockContent()) {
            int index = childFiles.indexOf(updateFile);
            if (index == -1)
                throw new IOException("Cannot update file attributes");
            updateFileRecord(index, updateFile);
        }
    }

    void ref(FATFile addFile) throws IOException {
        synchronized (fatFile.getLockContent()) {
            childFiles.add(addFile);
            updateFileRecord(childFiles.size() - 1, addFile);
        }
    }

    void deRef(FATFile removeFile) throws IOException {
        synchronized (fatFile.getLockContent()) {
            int offset = childFiles.indexOf(removeFile);
            if (offset == -1)
                throw new IOException("Cannot remove file from folder.");
            childFiles.set(offset, FATFile.DELETED_FILE);
            updateFileRecord(offset, FATFile.DELETED_FILE);
            //PERFORMANCE HINT POINT
            //todo: compact folder
        }
    }

}
