package com.test;

import com.test.FATBaseTest;
import com.test.FATFile;
import com.test.FATFileSystem;
import com.test.FATFolder;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

/**
 * Test for basic File Tree operations
 * User: uta
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

