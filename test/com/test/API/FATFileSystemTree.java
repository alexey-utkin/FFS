package com.test.API;

/**
 * Basic Directory Tree operations.
 * User: uta
 */

import com.test.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

public class FATFileSystemTree extends FATBaseTest {
    //
    // Test of FS open.
    //
    static public void testFileOpen(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException {
        startUp(path);

        final FATFileSystem ffs1  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType);
        FATFolder root1 = ffs1.getRoot();
        root1.createFolder("Test1");
        root1.createFolder("Test2");
        root1.createFolder("Test3");

        try {
            // test check name first, available space size
            root1.createFolder("Test2");
            throw new Error("Can create duplicates.");
        } catch (FileAlreadyExistsException ex) {
            //OK
        }

        ffs1.close();

        final FATFileSystem ffs2  = FATFileSystem.open(path);
        FATFolder root2 = ffs2.getRoot();
        if (root2.listFiles().length == 0)
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
        // = 1dir-empty + 1rec-in-root * 3folders count
        int clusterCount = 6; //fixed in minimal value
        for (int allocatorType : allocatorTypes) {
            for (int clusterSize : clusterSizes) {
                logStart(getPath(), clusterSize, clusterCount, allocatorType);
                testFileOpen(getPath(),
                        clusterSize, clusterCount, allocatorType);
                logOk();
            }
        }
    }

    //
    //  Test of FS tree test.
    //
    static public void testBaseTree(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException {
        startUp(path);

        final FATFileSystem ffs1  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType);
        FATFolder root1 = ffs1.getRoot();
        root1.createFolder("Test1")
                .createFolder("Test1_2")
                .createFolder("Test1_3");

        root1.deleteChildren();
        if (root1.findFile("Test1") != null)
            throw new Error("Bad deleteChildren call.");

        root1.createFolder("Test2");
        root1.createFolder("Test3");
        root1.pack();

        ffs1.close();

        final FATFileSystem ffs2  = FATFileSystem.open(path);
        FATFolder root2 = ffs2.getRoot();
        if (root2.listFiles().length != 2)
            throw new Error("Bad root.");
        ffs2.close();

        tearDown(path);
    }
    @Test
    public void testBaseTree() throws IOException {
        for (int allocatorType : allocatorTypes) {
            int clusterSize = FATFile.RECORD_SIZE*3/2; //fixed!
            int clusterCount = 400; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testBaseTree(getPath(), clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }
}
