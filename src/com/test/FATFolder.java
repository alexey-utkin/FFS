package com.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 */

public class FATFolder {
    private static final String ROOT_NAME = "<root>";
    //to check long in call params
    private static final long EMPTY_FILE_SIZE = 0L;
    private static final int  ROOT_FILE_ID = 0;
    final FATFile fatFile;

    //PERFORMANCE HINT POINT
    //use collections adapted for critical operations
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
            throw new IOException("Root already exists.");

        rootFile.initSize(FATFile.RECORD_SIZE);// have to be at least one record length
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
        FATFile rootFile = fs.openFile(FATFile.TYPE_FOLDER, ROOT_FILE_ID, ROOT_FILE_ID);
        // exclusive access to [ret]
        FATFolder ret = fs.getFolder(rootFile.getFileId());
        rootFile.initSize(FATFile.RECORD_SIZE);// have to be at least one record length
        ret.readContent();
        if (ret.childFiles.isEmpty()
            || !ROOT_NAME.equals(ret.childFiles.get(0).toString()))
            throw new IOException("Root folder is damaged!");
        return ret;
    }

    /**
     * Creates a child folder.
     *
     * @param folderName the name of folder.
     * @throws IOException
     */
    public void  createSubfolder(String folderName) throws IOException {
        synchronized (fatFile.getLockContent()) {
            try {
                fs().begin(true);
                FATFile subfolder = findFolder(folderName);
                if (subfolder != null)
                    throw new FileAlreadyExistsException(folderName);
                //reserve space fist!
                ref(FATFile.DELETED_FILE);

                // [access] is the same as in parent by default
                subfolder = fs().createFile(FATFile.TYPE_FOLDER, EMPTY_FILE_SIZE, fatFile.access());
                subfolder.initName(folderName);
                subfolder.moveTo(this);
            } finally {
                fs().end();
            }
        }
    }

    /**
     * Packs the folder in external memory.
     *
     * @return the number of bytes that were free.
     */
    public int pack() throws IOException {
        synchronized (fatFile.getLockContent()) {
            try {
                fs().begin(true);
                return 0;
            } finally {
                fs().end();
            }
        }
    }

    /***
     * Finds the file with selected name in folder collection.
     *
     * FUNCTIONAL HINT POINT: [folderName] as regexp
     * FUNCTIONAL HINT POINT: Hash map collection for fast Unique test.
     *
     * @param folderName the exact name to find, case sensitive.
     * @return the found file or [null].
     */
    public FATFile findFolder(String folderName) {
        if (folderName == null)
            return null;
        synchronized (fatFile.getLockContent()) {
            for (FATFile file : childFiles) {
                if (folderName.equals(file.toString()))
                    return file;
            }
        }
        return null;
    }

    private void readContent() throws IOException {
        synchronized (fatFile.getLockContent()) {
            try {
                fs().begin(false);
                try (FATFileChannel folderContent = fatFile.getChannel(false)) {
                    ByteBuffer bf = fs().allocateBuffer(FATFile.RECORD_SIZE);
                    // for the root folder the [fatFile.size]
                    // will updated at fist read to actual value.
                    while (folderContent.position() < fatFile.length()) {
                        folderContent.read(bf);
                        bf.flip();
                        childFiles.add(fs().openFile(bf, getFolderId()));
                        bf.position(0);
                    }
                    if (folderContent.position() != fatFile.length())
                        throw new IOException("Folder is damaged!");
                }
            } finally {
                fs().end();
            }
        }
    }

    public int getFolderId() {
        return fatFile.getFileId();
    }

    private void updateFileRecord(int index, FATFile updateFile) throws IOException {
        try (FATFileChannel folderContent = fatFile.getChannel(false)) {
            folderContent
                .position(index * FATFile.RECORD_SIZE)
                .write((ByteBuffer) updateFile
                        .serialize(fs().allocateBuffer(FATFile.RECORD_SIZE),
                                fs().getVersion())
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
            int pos = childFiles.indexOf(FATFile.DELETED_FILE);
            if (pos >= 0) {
                childFiles.set(pos, addFile);
            } else {
                childFiles.add(addFile);
                pos = childFiles.size() - 1;
            }
            updateFileRecord(pos, addFile);
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

    private FATFileSystem fs() {
        return fatFile.fs;
    }
}
