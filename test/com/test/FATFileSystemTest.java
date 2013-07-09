package com.test;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
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
    public void testFileAsFolder() throws IOException {
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
    // Test of FS Folder-as-file open.
    //
    static public void testFolderAsFile(Path path, int clusterSize, int clusterCount,
                                        int allocatorType) throws IOException {
        startUp(path);
        try (final FATFileSystem ffs = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            ffs.getRoot().createFolder("testFolder");
        }

        try (final FATFileSystem ffs = FATFileSystem.open(path)) {
            try {
                //That is ok.
                FATFile file = ffs.getRoot().getChildFile("testFolder");
                //That is not ok.
                file.getChannel(false);
                throw new IOException("Open Folder context as file");
            } catch (IOException ex) {
                //ok
            }
        }
        tearDown(path);
    }
    @Test
    public void testFolderAsFile() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE; //fixed!
        int clusterCount = 2000*3 + 1; //fixed!
        int allocatorType = allocatorTypes[0];
        logStart(getPath(), clusterSize, clusterCount, allocatorType);
        testFolderAsFile(getPath(),
                clusterSize, clusterCount, allocatorType);
        logOk();
    }


    //
    // Test of FS file disposer.
    //
    static public void testFileDisposer(Path path, int clusterSize, int clusterCount,
                                      int allocatorType) throws IOException {
        startUp(path);
        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            long freeSpace = ffs.getFreeSize();
            {
                FATFolder collector = ffs.getRoot().createFolder("collector");
                for (int i = 0; i < 4000; ++i) {
                    collector.createFile("file" + i);
                }
            }
            for (int i = 0; i < 10; ++i) {
                System.gc();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    //ok
                }
                logLN("subf File Cache Size:" + ffs.getFileCacheSize()
                        + " Folder Cache Size:" + ffs.getFolderCacheSize());
            }
            if ( ffs.getFileCacheSize() >= 4000 || ffs.getFolderCacheSize() >= 4000 )
                throw new Error("Memory leaks in subfolders!");
            {
                FATFolder root = ffs.getRoot();
                root.deleteChildren();
                root.pack();
                if (freeSpace != ffs.getFreeSize())
                    throw new Error("Space leak.");
            }

            for (int i = 0; i < 4000; ++i) {
                ffs.getRoot().createFile("file" + i);
            }
            for (int i = 0; i < 10; ++i) {
                System.gc();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    //ok
                }
                logLN("root File Cache Size:" + ffs.getFileCacheSize()
                    + " Folder Cache Size:" + ffs.getFolderCacheSize());
            }
            if ( ffs.getFileCacheSize() >= 4000 || ffs.getFolderCacheSize() >= 4000 )
                throw new Error("Memory leaks in root!");
        }
        tearDown(path);
    }
    @Test
    public void testFileDisposer() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE; //fixed!
        int clusterCount = 4000*3 + 1; //fixed!
        int allocatorType = allocatorTypes[0];

        logStart(getPath(), clusterSize, clusterCount, allocatorType);
        testFileDisposer(getPath(),
                clusterSize, clusterCount, allocatorType);
        logOk();
    }

    //
    // Test of FS file disposer 2.
    //
    static public void testFileDisposer2(Path path, int clusterSize, int clusterCount,
                                        int allocatorType) throws IOException {
        startUp(path);
        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFile ff[] = new FATFile[1];
            long freeSpace = ffs.getFreeSize();
            {
                int fi = 0;
                FATFolder collector = ffs.getRoot().createFolder("collector");
                for (int i = 0; i < 2000; ++i) {
                    FATFile f = collector.createFile("file" + i);
                    if (fi < ff.length) {
                        ff[fi++] = f;
                    }
                }
            }
            for (int i = 0; i < 100; ++i) {
                System.gc();
                logLN("File Cache Size:" + ffs.getFileCacheSize()
                   + " Folder Cache Size:" + ffs.getFolderCacheSize());
            }
            if ( ffs.getFileCacheSize() < ff.length)
                throw new Error("Cache lost!");

        }
        tearDown(path);
    }
    @Test
    public void testFileDisposer2() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE; //fixed!
        int clusterCount = 2000*3 + 1; //fixed!
        int allocatorType = allocatorTypes[0];

        logStart(getPath(), clusterSize, clusterCount, allocatorType);
        testFileDisposer2(getPath(),
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

    //
    // Test of FS Folder Dump.
    //
    static public void testFolderDump(Path path, int clusterSize, int clusterCount,
                                        int allocatorType) throws IOException {
        startUp(path);
        try (final FATFileSystem ffs = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root = ffs.getRoot();
            FATFolder html = root.createFolder("html");
            html.createFolder("head")
                    .createFile("title")
                        .getChannel(false)
                            .write(ByteBuffer.wrap("That was funny! That was funny! That was funny!".getBytes()));
            html.createFolder("body")
                    .createFile("context")
                       .getChannel(false)
                          .write(ByteBuffer.wrap("That fun!".getBytes()));
            log(root.getView());
        }
        tearDown(path);
    }
    @Test
    public void testFolderDump() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE;
        int clusterCount = 20;
        for (int allocatorType : allocatorTypes) {
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testFolderDump(getPath(),
                    clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

    //
    // Test of forward space reservation in folder store.
    //
    static public void testCreateNoSize(Path path, int clusterSize, int clusterCount,
                                                    int allocatorType) throws IOException {
        startUp(path);
        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root = ffs.getRoot();
            long freeSpace = ffs.getFreeSize();
            root.createFolder("Test1");
            //ok
            try {
                root.createFolder("Test2");
                throw new Error("Impossible allocation.");
            } catch (IOException ex) {
                //ok, but should not be dirty!
            }
            //check for dirty
            root.deleteChildren();
            root.pack();
            if (freeSpace != ffs.getFreeSize())
                throw new Error("Lost allocation.");

            //not root
        }
        tearDown(path);
    }
    @Test
    public void testCreateNoSize() throws IOException {
        int clusterSize = FATFile.RECORD_SIZE;
        int clusterCount = 2;
        for (int allocatorType : allocatorTypes) {
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testCreateNoSize(getPath(),
                    clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

}

