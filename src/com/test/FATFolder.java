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
    final FATFile fatFile;

    //PERFORMANCE HINT POINT
    //todo: use different collections for main operations
    final ArrayList<FATFile> childFiles = new ArrayList<>();


    /**
     * Creates Folder from File.
     *
     * Could not be called directly, use [fs.getFolder] instead.
     *
     * @param _fatFile
     */
    FATFolder(FATFile _fatFile) {
        fatFile = _fatFile;
    }


    static FATFolder createRoot(FATFileSystem fs, int access) throws IOException {
        // exclusive access to [ret]
        FATFile rootFile = fs.createFile(FATFile.TYPE_FOLDER, 0, access);
        if (rootFile.getFileId() != 0)
            new IOException("Root already exists.");

        rootFile.initName(ROOT_NAME);
        // self store
        FATFolder ret = fs.getFolder(rootFile.getFileId());
        rootFile.moveTo(ret);
        return ret;
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
