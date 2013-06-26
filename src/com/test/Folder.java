package com.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;

/**
 * Created with IntelliJ IDEA.
 * User: u
 * Date: 6/22/13
 * Time: 3:47 PM
 * To change this template use File | Settings | File Templates.
 */
class Folder {
    private final Object lockWrite = new Object();
    private FolderEntry firstEntry;

    protected Folder() {}

    Folder create(FATSystem fs, Folder parent, String name, int access) throws IOException {
        FolderEntry fe;        
        synchronized (lockWrite) {
            if (parent != null) {
                fe = parent.find(fs, name);
                if (fe != null)
                    throw new FileAlreadyExistsException(fe.toString());
            }
            // creates self-terminate entry with allocated cluster with timestamp
            fe = FolderEntry.create(fs, name, FolderEntry.TYPE_FOLDER, access);
            addEntry(fs, fe);
        }
        return createFromEntry(fs, fe);
    }

    private static Folder createFromEntry(FATSystem fs, FolderEntry fe) {
        Folder ret = new Folder();
        ret.firstEntry = fe;
        return ret;
    }

    private void addEntry(FATSystem fs, FolderEntry folderEntry) {
        //todo:
    }

    private FolderEntry find(FATSystem fs, String name) throws IOException {
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
}
