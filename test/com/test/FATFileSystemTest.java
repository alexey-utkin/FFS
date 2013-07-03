package com.test;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Tests for basic File System operations with root access.
 */

public class FATFileSystemTest  extends FATBaseTest {

    //
    // Test of FS creation.
    //
    static public void testFileCreate(Path path, int clusterSize, int clusterCount,
                                      int allocatorType) throws IOException {
        startUp(path);
        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {

        }
        tearDown(path);
    }
    @Test
    public void testFileCreate() throws IOException {
        final int[] clusterSizes = new int[] {
            FATFile.RECORD_SIZE,
            4096
        };
        int clusterCount = 2;
        for (int allocatorType : allocatorTypes) {
            for (int clusterSize : clusterSizes) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testFileCreate(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }

    //
    // Test of FS file-as-folder open.
    //
    static public void testFileAsFolder(Path path, int clusterSize, int clusterCount,
                                        int allocatorType) throws IOException {
        startUp(path);
        try (final FATFileSystem ffs = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            ffs.getRoot().createFile("test");
        }

        try (final FATFileSystem ffs = FATFileSystem.open(path)) {
            try {
                ffs.getRoot().getChildFolder("test");
                throw new IOException("Open File as folder");
            } catch (IOException ex) {
                //ok
            }
        }
        tearDown(path);
    }
    @Test
    public void testFileCreateOpen() throws IOException {
        final int[] clusterSizes = new int[] {
                FATFile.RECORD_SIZE,
                4096
        };
        int clusterCount = 2;
        for (int allocatorType : allocatorTypes) {
            for (int clusterSize : clusterSizes) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testFileAsFolder(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }

    //
    // Test of FS file diaposer.
    //
    static public void testFileDisposer(Path path, int clusterSize, int clusterCount,
                                      int allocatorType) throws IOException {
        startUp(path);
        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            //Root is always in memory - need to be fixed?
            long freeSpace = ffs.getFreeSize();
            {
                FATFolder collector = ffs.getRoot().createFolder("collector");
                for (int i = 0; i < 2000; ++i) {
                    collector.createFile("file" + i);
                }
            }    
            for (int i = 0; i < 10; ++i) {
                System.gc();                
                logLN("subf File Cach Size:" + ffs.getFileCachSize() 
                    + " Folder Cach Size:" + ffs.getFolderCachSize());
            }
            if ( ffs.getFileCachSize() >= 2000 || ffs.getFolderCachSize() >= 2000 )
                throw new Error("Memory leaks in subfolders!");

            {
                FATFolder root = ffs.getRoot();
                root.deleteChildren();
                root.pack();
                if (freeSpace != ffs.getFreeSize())
                    throw new Error("Space leak.");
            }

            for (int i = 0; i < 2000; ++i) {
                ffs.getRoot().createFile("file" + i);
            }
            for (int i = 0; i < 10; ++i) {
                System.gc();                
                logLN("root File Cach Size:" + ffs.getFileCachSize() 
                    + " Folder Cach Size:" + ffs.getFolderCachSize());
            }
            if ( ffs.getFileCachSize() >= 2000 || ffs.getFolderCachSize() >= 2000 )
                throw new Error("Memory leaks in root!");
        }
        tearDown(path);
    }
    @Test
    public void testFileDisposer() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE; //fixed!
        int clusterCount = 2000*3 + 1; //fixed!
        int allocatorType = allocatorTypes[0];

        logStart(getPath(), clusterSize, clusterCount, allocatorType);
        testFileDisposer(getPath(),
                clusterSize, clusterCount, allocatorType);
        logOk();
    }
    

    //
    // Test of forward space reservation in folder store.
    //
    static public void testForwardFolderReservation(Path path, int clusterSize, int clusterCount,
                                                    int allocatorType) throws IOException {
        startUp(path);
        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root1 = ffs.getRoot();
            //here: 2 free, 1 <root:empty> record is out of FAT

            try {
                root1.createFolder("Test1");
                //here: 1 free, 1 <root:full>, 1 <Test1:empty>
                root1.createFolder("Test2");
                //root is full, 2 free clusters need
                throw new Error("Impossible allocation.");
            } catch (IOException ex) {
                root1.pack();
                //check that we does not loose the space.
                if (ffs.getFreeSize() != FATFile.RECORD_SIZE)
                    throw new Error("Lost cluster on folder allocation.");
                root1.findFile("Test1").getFolder().createFolder("Test2");
                //here: 0 free, 1 <root:full>, 1 <Test1:full>, 1 <Test2:empty>
                if (ffs.getFreeSize() != 0)
                    throw new Error("Lost cluster on folder allocation 2.");
            }
        }
        tearDown(path);
    }
    @Test
    public void testForwardFolderReservation() throws IOException {
        for (int allocatorType : allocatorTypes) {
            int clusterSize = FATFile.RECORD_SIZE; //fixed!
            int clusterCount = 3; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testForwardFolderReservation(getPath(),
                    clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }
}

