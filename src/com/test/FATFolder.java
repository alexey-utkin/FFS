package com.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 *
 * All [transact-safe] methods have the [ts_] prefix.
 * The [transact-safe] means that methods can terminated successfully,
 *   or restore the object state to initial condition and throw exception,
 *   or mark system as "dirty" and throw exception (critical error in host FS).
 *
 * All public functions have to be [transact-safe] by default.
 */

public class FATFolder {
    private static final String ROOT_NAME = "<root>";

    // to check long in call params
    private static final long EMPTY_FILE_SIZE = 0L;

    // SIZE HINT POINT
    // FS guaranty, that deleted record less then a half.
    private int deletedCount = 0;
    // set this flag on folder delete
    private boolean packForbidden = false;

    final FATFile fatFile;

    //PERFORMANCE HINT POINT
    //use collections adapted for critical operations
    ArrayList<FATFile> childFiles = new ArrayList<>();

    /**
     * Creates a child folder.
     *
     * @param folderName
     * @return
     * @throws IOException
     */
    public FATFolder createSubfolder(String folderName) throws IOException {
        synchronized (fatFile.ts_getFileLock()) {
            fatFile.checkValid();
            ts_fs().begin(true);
            try {
                FATFile subfolder = findFile(folderName);
                if (subfolder != null)
                    throw new FileAlreadyExistsException(folderName);

                // reserve space in parent first!
                ts_ref(FATFile.DELETED_FILE);
                ++deletedCount;

                // [access] is the same as in parent by default
                subfolder = ts_fs().ts_createFile(folderName,
                        FATFile.TYPE_FOLDER, EMPTY_FILE_SIZE, fatFile.access());

                // need a lock from delete in dirty state.
                synchronized (subfolder.ts_getFileLock()) {
                    boolean success = false;
                    try {
                        //since this call the [subfolder] is visible for external world
                        subfolder.moveTo(this);
                        // commit
                        success = true;
                    } finally {
                        if (!success) {
                            // rollback
                            boolean successRollback = false;
                            try {
                                ts_fs().ts_dropDirtyFile(subfolder);
                            } finally {
                                if (!successRollback) {
                                    ts_fs().ts_setDirtyState("Cannot drop unconnected folder.", false);
                                }
                            }
                        } else {
                            return subfolder.getFolder();
                        }
                    }
                }
            } finally {
                ts_fs().end();
            }
        }
        return null;
    }

    /**
     * Deletes all children.
     *
     * Deletes all child files and folders.
     * To delete a folder the {@link #cascadeDelete()} function is called.
     *
     * @throws IOException
     */
    public void deleteChildren() throws IOException {
        synchronized (fatFile.ts_getFileLock()) {
            fatFile.checkValid();
            boolean success = false;
            ts_fs().begin(true);
            try {
                packForbidden = true;
                int capacity = childFiles.size();
                for (int i = 0; i < capacity; ++i) {
                    FATFile current = childFiles.get(i);
                    if (current != FATFile.DELETED_FILE) {
                        if (current.isFolder())
                            current.getFolder().cascadeDelete();
                        else
                            current.delete();
                    }
                }
                // commit
                success = true;
            } finally {
                packForbidden = false;
                // partial delete
                if (!success)
                    ts_optionalPack();
                ts_fs().end();
            }
        }
    }

    /**
     * Cascade folder delete.
     *
     * Delete process stops on the first error. No rollback.
     * Can be called for [root]: that removes children and throw
     * an exception while root file delete.
     *
     * @throws IOException
     */
    public void cascadeDelete() throws IOException {
        synchronized (fatFile.ts_getFileLock()) {
            fatFile.checkValid();
            boolean success = false;
            ts_fs().begin(true);
            try {
                deleteChildren();
                // commit
                fatFile.delete();
            } finally {
                ts_fs().end();
            }
        }
    }

    /**
     * Packs the folder in external memory.
     *
     * @return the number of bytes that were free.
     */
    public int pack() throws IOException {
        synchronized (fatFile.ts_getFileLock()) {
            fatFile.checkValid();

            // PERFORMANCE HINT POINT
            // make it better with alternative collection
            int startSize = childFiles.size();
            ArrayList<FATFile> _childFiles = new ArrayList<FATFile>();
            for (int i = 0; i < startSize; ++i) {
                FATFile current = childFiles.get(i);
                if (current != FATFile.DELETED_FILE) {
                    _childFiles.add(current);
                }
            }
            _childFiles.trimToSize();

            int endSize = _childFiles.size();
            if (startSize != endSize) {
                //write first, truncate after!
                ts_fs().begin(true);
                try {
                    childFiles = _childFiles;
                    deletedCount = 0;
                    ts_writeContent();
                    //commit transaction
                } finally {
                    ts_fs().end();
                }
            }
            return startSize - endSize;
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
    public FATFile findFile(String folderName) {
        synchronized (fatFile.ts_getFileLock()) {
            fatFile.checkValid();
            if (folderName == null)
                return null;
            for (FATFile file : childFiles) {
                if (folderName.equals(file.toString()))
                    return file;
            }
        }
        return null;
    }


    /**
     * Creates Folder from File.
     *
     * Could not be called directly, use [ts_fs().ts_getFolder] instead.
     *
     * @param fatFile the folder storage
     */
    FATFolder(FATFile fatFile) {
        this.fatFile = fatFile;
    }


    /**
     * Creates root folder on empty storage.
     *
     * @param fs the File System to mount in.
     * @param access the desired access
     * @return the Root Folder object
     * @throws IOException
     */
    static FATFolder ts_createRoot(FATFileSystem fs, int access) throws IOException {
        // exclusive access to [ret]
        boolean success = false;
        try {
            FATFile rootFile = fs.ts_createFile(ROOT_NAME, FATFile.TYPE_FOLDER, EMPTY_FILE_SIZE, access);
            if (rootFile.ts_getFileId() != FATFile.ROOT_FILE_ID)
                throw new IOException("Root already exists.");
            FATFolder ret = fs.ts_getFolder(rootFile.ts_getFileId());
            // update record in header
            rootFile.setLastModified(FATFileSystem.getCurrentTime());
            // commit
            success = true;
            return ret;
        } finally {
            if (!success) {
                // primitive rollback
                fs.ts_setDirtyState("Cannot create the root folder.", false);
            }
        }
    }

    /**
     * Opens existent root folder.
     *
     * @param fs the File System to read from.
     * @return the Root Folder object
     * @throws IOException
     */
    static FATFolder ts_openRoot(FATFileSystem fs) throws IOException {
        // exclusive access to [ret]
        boolean success = false;
        try {
            FATFile rootFile = fs.ts_openFile(fs.getRootInfo(), FATFile.ROOT_FILE_ID);
            if (rootFile.ts_getFileId() != FATFile.ROOT_FILE_ID
                || !ROOT_NAME.equals(rootFile.toString()))
            {
                throw new IOException("Root folder is damaged!");
            }

            FATFolder ret = fs.ts_getFolder(rootFile.ts_getFileId());
            ret.ts_readContent();
            success = true;
            return ret;
        } finally {
            if (!success) {
                // primitive rollback
                fs.ts_setDirtyState("Cannot open the root folder.", false);
            }
        }
    }

    private void ts_readContent() throws IOException {
        boolean success = false;
        synchronized (fatFile.ts_getFileLock()) {
            ts_fs().begin(false);
            try {
                try (FATFileChannel folderContent = fatFile.getChannel(false)) {
                    ByteBuffer bf = ts_fs().ts_allocateBuffer(FATFile.RECORD_SIZE);
                    long storageSize = fatFile.length();
                    while (folderContent.position() < storageSize) {
                        folderContent.read(bf);
                        bf.flip();
                        childFiles.add(ts_fs().ts_openFile(bf, ts_getFolderId()));
                        bf.position(0);
                    }
                    if (folderContent.position() != storageSize)
                        throw new IOException("Folder is damaged!");
                    success = true;
                }
            } finally {
                if (!success) {
                    //primitive rollback - cannot restore.
                    ts_fs().ts_setDirtyState("Cannot read folder content", false);
                }
                ts_fs().end();
            }
        }
    }

    private void ts_writeContent() throws IOException {
        boolean success = false;
        synchronized (fatFile.ts_getFileLock()) {
            ts_fs().begin(true);
            try {
                try (FATFileChannel folderContent = fatFile.getChannel(false)) {
                    ByteBuffer bf = ts_fs().ts_allocateBuffer(FATFile.RECORD_SIZE);
                    for (FATFile file : childFiles) {
                        bf.position(0);
                        file.ts_serialize(bf, ts_fs().getVersion());
                        bf.flip();
                        folderContent.write(bf);
                    }
                    //update size in parent
                    fatFile.setLength(folderContent.position());
                    success = true;
                }
            } finally {
                if (!success) {
                    //primitive rollback - cannot restore.
                    ts_fs().ts_setDirtyState("Cannot write folder content", false);
                }
                ts_fs().end();
            }
        }
    }

    /**
     * Updates the [index] element in folder storage.
     *
     * @param index
     * @param updateFile
     * @throws IOException
     */
    private void ts_updateFileRecord(int index, FATFile updateFile) throws IOException {
        boolean success = false;
        try (FATFileChannel folderContent = fatFile.getChannel(false)) {
            folderContent
                .position(index * FATFile.RECORD_SIZE)
                .write(
                    (ByteBuffer) updateFile
                        .ts_serialize(
                            ts_fs().ts_allocateBuffer(FATFile.RECORD_SIZE),
                            ts_fs().getVersion())
                        .flip());
            // commit
            success = true;
        } finally {
            if (!success) {
                //primitive rollback - cannot restore (not [ts_] function call in action).
                ts_fs().ts_setDirtyState("Cannot read folder record", false);
            }
        }
    }

    private void ts_updateRootFileRecord(FATFile rootFile) throws IOException {
        boolean success = false;
        try {
            ts_fs().updateRootRecord(
                    (ByteBuffer) rootFile
                            .ts_serialize(
                                    ts_fs().ts_allocateBuffer(FATFile.RECORD_SIZE),
                                    ts_fs().getVersion())
                            .flip());
            // commit
            success = true;
        } finally {
            if (!success) {
                //primitive rollback - cannot restore (not [ts_] function call in action).
                ts_fs().ts_setDirtyState("Cannot update root folder record", false);
            }
        }
    }

    void ts_updateFileRecord(FATFile updateFile) throws IOException {
        synchronized (fatFile.ts_getFileLock()) {
            fatFile.checkValid();
            if (updateFile.isRoot()) {
                ts_updateRootFileRecord(updateFile);
            } else {
                int index = childFiles.indexOf(updateFile);
                if (index == -1)
                    throw new IOException("Cannot update file attributes");
                ts_updateFileRecord(index, updateFile);
            }
        }
    }


    void ts_ref(FATFile addFile) throws IOException {
        synchronized (fatFile.ts_getFileLock()) {
            fatFile.checkValid();
            int pos = childFiles.indexOf(FATFile.DELETED_FILE);
            if (pos >= 0) {
                childFiles.set(pos, addFile);
                --deletedCount;
            } else {
                childFiles.add(addFile);
                pos = childFiles.size() - 1;
            }
            ts_updateFileRecord(pos, addFile);
        }
    }

    void ts_deRef(FATFile removeFile) throws IOException {
        synchronized (fatFile.ts_getFileLock()) {
            fatFile.checkValid();
            int offset = childFiles.indexOf(removeFile);
            if (offset == -1)
                throw new IOException("Cannot remove file from folder.");
            childFiles.set(offset, FATFile.DELETED_FILE);
            ++deletedCount;
            ts_updateFileRecord(offset, FATFile.DELETED_FILE);
            ts_optionalPack();
        }
    }

    private void ts_optionalPack() throws IOException {
        //SIZE HINT POINT
        //compact folder
        if (deletedCount > (childFiles.size() >> 1))
            pack();
    }

    int ts_getFolderId() {
        return fatFile.ts_getFileId();
    }

    private FATFileSystem ts_fs() {
        return fatFile.fs;
    }
}
