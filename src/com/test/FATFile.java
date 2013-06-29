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
    public static final int FILE_MAX_NAME = 127;

    public static final int TYPE_FILE = 0;
    public static final int TYPE_FOLDER = 1;

    final FATFileSystem fs;

    public static final int RECORD_SIZE = 3*4 + 3*8 + FILE_MAX_NAME*2;
    int  fileID;
    int  type;
    int  access;
    long size;
    long timeCreate;
    long timeModify;
    final char[] name = new char[FILE_MAX_NAME];

    void initName(String _name) throws IOException {
        int len = _name.length();
        if (len > FILE_MAX_NAME)
                throw new IOException("Name is too long:" + _name);
        Arrays.fill(name, (char)0xCDCD);
        System.arraycopy(_name.toCharArray(), 0, name, 0, len);
    }

    protected FATFile(FATFileSystem _fat, int _type, long _size, int _access) throws IOException {
        fs = _fat;
        size = _size;
        type = _type;
        access = _access;
        timeCreate = FATFileSystem.getCurrentTime();
        timeModify = FATFileSystem.getCurrentTime();
        fileID = fs.allocateFileSpace(size);
    }

    public FATFileChannel getChannel(boolean appendMode) {
        return new FATFileChannel(this, appendMode);
    }

    public void delete() throws IOException {
        fs.deleteFile(this);
    }

    protected ByteBuffer serialize(ByteBuffer bf, int version) {
        bf
            .putInt(fileID)
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
    public synchronized void setLength(long newLength) throws IOException {
        if (newLength == size)
            return;
        fs.setFileLength(this, newLength);
        size = newLength;
    }

}
