package com.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: u
 * Date: 6/22/13
 * Time: 3:42 PM
 * To change this template use File | Settings | File Templates.
 */
class FolderEntry {
    public static final int FILE_MAX_NAME = 127;
    public static final int RECORD_SIZE = 3*4 + 3*8 + FILE_MAX_NAME*2;

    public static final int TYPE_FILE = 0;
    public static final int TYPE_FOLDER = 1;

    int  startCluster;
    int  type;
    int  access;
    long size;
    long timeCreate;
    long timeModify;
    final char[] name = new char[FILE_MAX_NAME];

    public FolderEntry() {}

    //depends from FS version
    void serialize(ByteBuffer bf, int version) {
        bf
            .putInt(startCluster)
            .putLong(size)
            .putLong(timeCreate)
            .putLong(timeModify)
            .putInt(access)
            .putInt(type);
        // only UNICODE name for performance and compatibility reasons
        bf.asCharBuffer().put(name);
    }

    void restore(ByteBuffer bf, int version) {
        startCluster = bf.getInt();
        size = bf.getLong();
        type = bf.getInt();
        access = bf.getInt();
        timeCreate = bf.getLong();
        timeModify = bf.getLong();
        // only UNICODE name for performance and compatibility reasons
        bf.asCharBuffer().get(name);
    }

    @Override
    public String toString() {
        String ret = new String(name);
        int zeroPos = ret.indexOf(0);
        return (zeroPos == -1)
            ? ret
            : ret.substring(0, zeroPos);
    }

    /**
     *  Creates self-terminate entry with allocated cluster with timestamp
     *
     * @param fs
     * @param name
     * @param type_folder
     * @return
     */
    public static FolderEntry create(FileSystem fs, String name, int type_folder, int access) throws IOException {
        FolderEntry ret = new FolderEntry();
        //ret.startCluster = fs.allocateClusters(-1, 1);
        ret.size = 0;
        ret.type = type_folder;
        ret.access = access;
        //ret.timeCreate = fs.getCurrentTime();
        ret.timeModify = ret.timeCreate;

        int len = name.length();
        if (len > FILE_MAX_NAME)
            throw new IllegalArgumentException("Name is too long:" + name);

        Arrays.fill(ret.name, '0');
        System.arraycopy(name.toCharArray(), 0, ret.name, 0, len);
        return ret;
    }

    public static FolderEntry read(ByteBuffer bf, int version) {
        FolderEntry ret = new FolderEntry();
        ret.restore(bf, version);
        return ret;
    }
}
