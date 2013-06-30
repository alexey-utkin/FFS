package com.test;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 */
public class FATFile {
    public static final int INVALID_FILE_ID = -1;
    public static final int FILE_MAX_NAME = 110;
    public static final char ZAP_CHAR = 0xCDCD;

    public static final int TYPE_FILE = 0;
    public static final int TYPE_FOLDER = 1;
    public static final int TYPE_DELETED = -1;

    static final FATFile DELETED_FILE = new FATFile(null, TYPE_DELETED, INVALID_FILE_ID, INVALID_FILE_ID);

    final FATFileSystem fs;

    //lockAttribute ->lockContent lock sequence
    private final Object lockAttribute = new Object();
    private final Object lockContent = new Object();


    public static final int RECORD_SIZE = 3*4 + 3*8 + FILE_MAX_NAME*2;  //256 bytes
    // attributes
    private final int type;
    private int fileId;
    private int  access;
    private long size;
    private long timeCreate;
    private long timeModify;
    private final char[] name = new char[FILE_MAX_NAME];

    // properties
    private int parentId = INVALID_FILE_ID;

    /**
     * Opens file from id
     */
    FATFile(FATFileSystem _fs, int _type, int _fileId, int _parentId) {
        fs = _fs;
        type = _type;
        fileId = _fileId;
        parentId = _parentId;
    }

    /**
     * Creates File in FAT with allocated space.
     *
     * Could not be called directly, use [fs.ts_createFile] instead.
     *
     * @param _fs
     * @param _type
     * @param _size
     * @param _access
     * @throws IOException
     */
    FATFile(FATFileSystem _fs, int _type, long _size, int _access) throws IOException {
        fs = _fs;
        fileId = fs.allocateFileSpace(size);
        type = _type;
        size = _size;
        timeCreate = FATFileSystem.getCurrentTime();
        timeModify = FATFileSystem.getCurrentTime();
        access = _access;
    }

    public void initFromBuffer(ByteBuffer bf) {
        // [fileId] and [type] was read before for [ctr] call
        size = bf.getLong();
        timeCreate = bf.getLong();
        timeModify = bf.getLong();
        access = bf.getInt();
        // only UNICODE name for performance and compatibility reasons
        bf.asCharBuffer().get(name);
    }


    public FATFileChannel ts_getChannel(boolean appendMode) {
        return new FATFileChannel(this, appendMode);
    }

    public void delete() throws IOException {
        throw new NotImplementedException();
    }

    ByteBuffer serialize(ByteBuffer bf, int version) {
        bf
            .putInt(fileId)
            .putInt(type)
            .putLong(size)
            .putLong(timeCreate)
            .putLong(timeModify)
            .putInt(access);
        // only UNICODE name for performance and compatibility reasons
        bf.asCharBuffer().put(name);
        bf.position(bf.position() + name.length*2);
        return bf;
    }

    public void force(boolean updateMetadata) throws IOException {

        if (updateMetadata)
            setLastModified(FATFileSystem.getCurrentTime());
        fs.ts_forceFileContent(this, updateMetadata);
    }

    /**
     * Updates attribute info in parent record if any.
     *
     * Have to be called under [lockAttribute].
     * @throws IOException
     */
    private void updateAttributes() throws IOException {
        if (parentId != INVALID_FILE_ID) {
            fs.ts_getFolder(parentId).ts_updateFileRecord(this);
        }
    }

    /**
     * Moves file to new location.
     *
     * @param newParent new owner of the file
     * @throws IOException
     */
    public void moveTo(FATFolder newParent) throws IOException {
        synchronized (lockAttribute) {
            boolean success = false;
            int oldParentId = parentId;
            try {
                fs.begin(true);
                //redirect any file attribute change (ex. size) to new parent
                parentId = newParent.getFolderId();
                newParent.ts_ref(this);
                if (oldParentId != INVALID_FILE_ID) {
                    try {
                        fs.ts_getFolder(oldParentId).ts_deRef(this);
                        // commit
                        success = true;
                    } finally {
                        if (!success) {
                            //rollback process
                            boolean successRollback = false;
                            try {
                                newParent.ts_deRef(this);
                                successRollback = true;
                            } finally {
                                if (!successRollback) {
                                    fs.ts_setDirtyState("Cannot rollback movement of the file. ", false);
                                }
                            }
                        }
                    }
                }
            } finally {
                if (!success) {
                    // rollback
                    parentId = oldParentId;
                    if (oldParentId == INVALID_FILE_ID) {
                        // it was file-to-parent binding
                        fs.ts_setDirtyState("Lost file problem.", false);
                    }
                }
                fs.end();
            }
        }
    }


    public String toString() {
        String ret = new String(name);
        int zeroPos = ret.indexOf(ZAP_CHAR);
        return (zeroPos == -1)
                ? ret
                : ret.substring(0, zeroPos);
    }

    void ts_initSize(long size) {
        this.size = size;
        //no update here! That is init!
    }

    void ts_initName(String fileName) {
        int len = fileName.length();
        if (len > FILE_MAX_NAME)
            throw new IllegalArgumentException("Name is too long. Max length is " + FILE_MAX_NAME);
        synchronized (lockAttribute) {
            Arrays.fill(name, ZAP_CHAR);
            System.arraycopy(fileName.toCharArray(), 0, name, 0, len);
            //no update here! That is init!
        }
    }

    /**
     * Gets the file length.
     *
     * @return the file length
     */
    public long length() {
        return size;
    }

    /**
     * Sets the length of this file.
     *
     * If the present length of the file as returned by the length method is
     * greater than the newLength argument then the file will be truncated.
     *
     * If the present length of the file as returned by the length method
     * is smaller than the newLength argument then the file will be extended.
     * In this case, the contents of the extended portion of the file are not defined.
     *
     * @param newLength The desired length of the file
     */
    public void setLength(long newLength) throws IOException {
        synchronized (lockAttribute) {
            synchronized (lockContent) {
                try {
                    fs.begin(true);
                    if (newLength == size)
                        return;
                    fs.setFileLength(this, newLength, size);
                    updateAttributes();
                    // commit
                    size = newLength;
                } finally {
                    fs.end();
                }
            }
        }
    }

    int getFileId() {
        return fileId;
    }

    void setFileId(int fileId) {
        this.fileId = fileId;
    }


    public boolean isFolder() {
        return type == TYPE_FOLDER;
    }

    Object getLockContent() {
        return lockContent;
    }

    Object getLockAttribute() {
        return lockAttribute;
    }

    /**
     * Gets the file access attribute.
     *
     * @return the file access state.
     */
    public int access() {
        return access;
    }

    /**
     * Sets the file access attribute.
     *
     * @param access the file access state.
     */
    public void setAccess(int access) throws IOException {
        if (this.access == access)
            return;
        synchronized (lockAttribute) {
            try {
                fs.begin(true);
                updateAttributes();
                // commit
                this.access = access;
            } finally {
                fs.end();
            }
        }
    }

    /**
     * Gets the file creation time attribute.
     *
     * @return the file creation time in milliseconds.
     */
    public long timeCreate() {
        return timeCreate;
    }

    /**
     * Sets the file creation time attribute.
     *
     * @param timeCreate the file creation time in milliseconds.
     */
    public void setTimeCreate(long timeCreate) throws IOException {
        if (this.timeCreate == timeCreate)
            return;
        synchronized (lockAttribute) {
            try {
                fs.begin(true);
                updateAttributes();
                // commit
                this.timeCreate = timeCreate;
            } finally {
                fs.end();
            }
        }
    }

    /**
     * Gets the file modification time attribute.
     *
     * @return the file modification time in milliseconds.
     */
    public long lastModified() {
        return timeModify;
    }

    /**
     * Sets the file modification time attribute.
     *
     * @param timeModify the file modification time in milliseconds.
     */
    public void setLastModified(long timeModify) throws IOException {
        if (this.timeModify == timeModify)
            return;
        synchronized (lockAttribute) {
            try {
                fs.begin(true);
                updateAttributes();
                // commit
                this.timeModify = timeModify;
            } finally {
                fs.end();
            }
        }
    }
}
