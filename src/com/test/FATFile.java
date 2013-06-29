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

    public static final int TYPE_FILE = 0;
    public static final int TYPE_FOLDER = 1;
    public static final int TYPE_DELETED = -1;

    static final FATFile DELETED_FILE = new FATFile();

    final FATFileSystem fs;

    //lockAttribute ->lockContent lock sequence
    private final Object lockAttribute = new Object();
    private final Object lockContent = new Object();


    public static final int RECORD_SIZE = 3*4 + 3*8 + FILE_MAX_NAME*2;  //256 bytes
    // attributes
    final int type;
    int fileId;
    int  access;
    long size;
    long timeCreate;
    long timeModify;
    final char[] name = new char[FILE_MAX_NAME];

    // properties
    private int parentId = INVALID_FILE_ID;


    void initName(String _name) throws IOException {
        int len = _name.length();
        if (len > FILE_MAX_NAME)
                throw new IOException("Name is too long:" + _name);
        Arrays.fill(name, (char)0xCDCD);
        System.arraycopy(_name.toCharArray(), 0, name, 0, len);
    }

    /**
     * For deleted instance only!
     */
    protected FATFile() {
        fs = null;
        type = TYPE_DELETED;
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
        size = _size;
        type = _type;
        access = _access;
        timeCreate = FATFileSystem.getCurrentTime();
        timeModify = FATFileSystem.getCurrentTime();
        fileId = fs.allocateFileSpace(size);
    }

    public FATFileChannel getChannel(boolean appendMode) {
        return new FATFileChannel(this, appendMode);
    }

    public void delete() throws IOException {
        //todo: folder case
        fs.deleteFile(this);
    }

    protected ByteBuffer serialize(ByteBuffer bf, int version) {
        bf
            .putInt(fileId)
            .putLong(size)
            .putLong(timeCreate)
            .putLong(timeModify)
            .putInt(access)
            .putInt(type);
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
                if (newLength == size)
                    return;
                fs.setFileLength(this, newLength);
                size = newLength;
                updateAttributes();
            }
        }
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

    public void moveTo(FATFolder newParent) throws IOException {
        synchronized (lockAttribute) {
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
        }
    }

    int getFileId() {
        return fileId;
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
}
