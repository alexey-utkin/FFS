package com.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 */

class FATFolder extends FATFile {

    protected FATFolder(FATFileSystem _fs, int _type, long _size, int _access) throws IOException {
        super(_fs, _type, _size, _access);
    }

    static FATFolder createRoot(FATFileSystem fs, int access) throws IOException {
        // exclusive access to [ret]
        FATFolder ret = new FATFolder(fs, TYPE_FOLDER, RECORD_SIZE, access);
        try (FATFileChannel cn = ret.getChannel(false)) {
            boolean completed = false;
            // create [.]
            ret.initName(".");
            cn.write(
                (ByteBuffer) ret.serialize(fs.allocateBuffer(RECORD_SIZE), fs.getVersion())
                .flip());
            ret.initName("<root>");
        }
        return ret;
    }

    public static FATFolder create(FATFolder parent, String name, int access) throws IOException {
        return null;
    }


/*
    private FolderEntry find(FileSystem fs, String name) throws IOException {
        synchronized (lockWrite) {
            int cluster = firstEntry.startCluster;
            while (true) {
                ByteBuffer bf = fs.readCluster(cluster);
                int entryCount = fs.getEntryPerCluster();
                for (int i = 0; i < entryCount; ++i) {
                    FolderEntry fe = FolderEntry.read(bf, fs.getVersion());
                    // deleted?
                    if (fe.toString().equals(name))
                        return fe;
                }
                int value = fs.readFatEntry(cluster);
                // end of Folder
                if ( (value & FATClusterAllocator.CLUSTER_EOC) == FATClusterAllocator.CLUSTER_EOC )
                    break;
                // can continue
                if ( (value & FATClusterAllocator.CLUSTER_ALLOCATED) == 0) {
                    throw new FileSystemException("Bad cluster value (free) Cluster#:" + cluster);
                }

                cluster = value & FATClusterAllocator.CLUSTER_INDEX;
            }
        }
        return null;
    }
*/
}
