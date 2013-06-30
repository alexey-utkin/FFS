package com.test;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

/**
 * Created with IntelliJ IDEA.
 * User: uta
 */

public class FATFileSystemTest  extends FATBaseTest {
    /**
     * Test of FS creation.
     */
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

    /**
     * Test of forward space reservation in folder store.
     */
    static public void testForwardFolderReservation(Path path, int clusterSize, int clusterCount,
                                                    int allocatorType) throws IOException {
        startUp(path);

        try (FATFileSystem ffs  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root1 = ffs.getRoot();
            //1-filled cluster - <root>,  1 free

            try {
                //need 2 cluster: 1 for a record in root, 1 for new folder, but only 1 exists
                root1.createSubfolder("Test1");
                throw new Error("Impossible allocation.");
            } catch (IOException ex) {
                //check that we does not loose the space.
                root1.pack();
                if (ffs.getFreeSize() != FATFile.RECORD_SIZE)
                    throw new Error("Lost cluster on folder allocation.");

            }

        }
        tearDown(path);
    }
    @Test
    public void testForwardFolderReservation() throws IOException {
        for (int allocatorType : allocatorTypes) {
            int clusterSize = FATFile.RECORD_SIZE; //fixed!
            int clusterCount = 2; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testForwardFolderReservation(getPath(),
                    clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

    /**
     * Test of FS open.
     */
    static public void testFileOpen(Path path, int clusterSize, int clusterCount,
                                      int allocatorType) throws IOException {
        startUp(path);

        final FATFileSystem ffs1  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType);
        FATFolder root1 = ffs1.getRoot();
        root1.createSubfolder("Test1");
        root1.createSubfolder("Test2");
        root1.createSubfolder("Test3");

        try {
            // test check name first, available space size
            root1.createSubfolder("Test2");
            throw new Error("Can create duplicates.");
        } catch (FileAlreadyExistsException ex) {
            //OK
        }

        ffs1.close();

        final FATFileSystem ffs2  = FATFileSystem.open(path);
        FATFolder root2 = ffs2.getRoot();
        if (root2.childFiles.isEmpty())
            throw new Error("Bad root.");
        ffs2.close();

        tearDown(path);
    }
    @Test
    public void testFileOpen() throws IOException {
        final int[] clusterSizes = new int[] {
                FATFile.RECORD_SIZE,
                4096
        };
        int clusterCount = 2/*dir + rec*/*3/*empty*/ + 1/*root*/; //fixed in minimal value
        for (int allocatorType : allocatorTypes) {
            for (int clusterSize : clusterSizes) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testFileOpen(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }

}
