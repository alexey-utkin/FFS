package com.test;

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
     * Could not be called directly, use [fs.createFile] instead.
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


    public FATFileChannel getChannel(boolean appendMode) {
        return new FATFileChannel(this, appendMode);
    }

    public void delete() throws IOException {
        fs.deleteFile(this);
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

    void force(boolean updateMetadata) throws IOException {
        if (updateMetadata)
            timeModify = FATFileSystem.getCurrentTime();
        fs.forceFileContent(this, updateMetadata);
    }

    /**
     * Updates attribute info in parent record if any.
     *
     * Have to be called under [lockAttribute].
     * @throws IOException
     */
    private void updateAttributes() throws IOException {
        if (parentId != INVALID_FILE_ID) {
            fs.getFolder(parentId).updateFileRecord(this);
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
            try {
                fs.begin(true);
                //redirect any file attribute change (ex. size) to new parent
                int oldParentId = parentId;
                parentId = newParent.getFolderId();
                boolean success = false;
                try {
                    newParent.ref(this);
                    success = true;
                } finally {
                    if (!success) {
                        parentId = oldParentId;
                    } else if (oldParentId != INVALID_FILE_ID) {
                        fs.getFolder(oldParentId).deRef(this);
                    }
                }
            } finally {
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

    void initSize(long size) {
        this.size = size;
        //no update here! That is init!
        //updateAttributes();
    }

    void initName(String fileName) {
        int len = fileName.length();
        if (len > FILE_MAX_NAME)
            throw new IllegalArgumentException("Name is too long. Max length is " + FILE_MAX_NAME);
        synchronized (lockAttribute) {
            Arrays.fill(name, ZAP_CHAR);
            System.arraycopy(fileName.toCharArray(), 0, name, 0, len);
            //no update here! That is init!
            //updateAttributes();
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
                    size = newLength;
                    updateAttributes();
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
                this.access = access;
                updateAttributes();
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
                this.timeCreate = timeCreate;
                updateAttributes();
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
                this.timeModify = timeModify;
                updateAttributes();
            } finally {
                fs.end();
            }
        }
    }
}
