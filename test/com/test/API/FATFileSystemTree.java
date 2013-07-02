package com.test.API;

/**
 * Basic Directory Tree operations.
 * User: uta
 */

import com.test.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

public class FATFileSystemTree extends FATBaseTest {
/*
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
*/
    //
    //  Test of FS tree.
    //
    static public void testBaseTree(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException {
        startUp(path);

        final FATFileSystem ffs1  = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType);

        FATFolder root1 = ffs1.getRoot();
        root1.createFolder("Test1")
                .createFolder("Test1_1")
                    .createFolder("Test1_1_1");
        root1.createFolder("Test2")
                .createFolder("Test2_1")
                    .createFolder("Test2_1_1");
        root1.createFolder("Test3");
        String dump1 = root1.getView();

        ffs1.close();

        final FATFileSystem ffs2  = FATFileSystem.open(path);
        FATFolder root2 = ffs2.getRoot();
        root2.getChildFolder("Test1")
                .getChildFolder("Test1_1")
                    .getChildFolder("Test1_1_1");
        root2.getChildFolder("Test2")
                .getChildFolder("Test2_1")
                    .getChildFolder("Test2_1_1");
        root2.getChildFolder("Test3");

        String dump2 = root2.getView();
        ffs2.close();

        tearDown(path);
    }
    @Test
    public void testBaseTree() throws IOException {
        for (int allocatorType : allocatorTypes) {
            int clusterSize = FATFile.RECORD_SIZE*2; //fixed!
            int clusterCount = 400; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testBaseTree(getPath(), clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

    //
    //  Test of FS tree move.
    //
    static public void testFileMove(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException {
        startUp(path);

        String dump1;
        try (final FATFileSystem ffs = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root1 = ffs.getRoot();

            //1->1_1->1_1_1
            root1.createFolder("1")
                    .createFolder("1_1")
                        .createFolder("1_1_1");
            //2->2_1->2_1_1
            root1.createFolder("2")
                    .createFolder("2_1")
                        .createFolder("2_1_1");

            //1->1_1->1_1_1
            //2->2_1->2_1_1
            root1.getChildFolder("2")
                    .getChildFile("2_1")
                        .moveTo(
            root1.getChildFolder("1"));
            //1->1_1->1_1_1
            // ->2_1->2_1_1
            //2

            dump1 = root1.getView();
        }

        try (final FATFileSystem ffs = FATFileSystem.open(path)) {
            FATFolder root1 = ffs.getRoot();

            root1.getChildFolder("1")
                    .getChildFolder("2_1")
                       .getChildFile("2_1_1");

            if (root1.getChildFolder("2").listFiles().length != 0)
                throw new Error("Not empty child list!");

            if (root1.getChildFolder("1").listFiles().length != 2)
                throw new Error("Wrong child list!");

            if (!dump1.equals(root1.getView()))
                throw new Error("Wrong dump!");
        }

        tearDown(path);
    }
    @Test
    public void testFileMove() throws IOException {
        for (int allocatorType : allocatorTypes) {
            int clusterSize = FATFile.RECORD_SIZE*3/2; //fixed!
            int clusterCount = 20; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testFileMove(getPath(), clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

    //
    //  Test of FS tree delete.
    //
    static public void testFileDelete(Path path, int clusterSize, int clusterCount,
                                    int allocatorType) throws IOException {
        startUp(path);

        String dump1;
        try (final FATFileSystem ffs = FATFileSystem.create(path, clusterSize, clusterCount, allocatorType)) {
            FATFolder root1 = ffs.getRoot();

            //1->1_1->1_1_1
            root1.createFolder("1")
                    .createFolder("1_1")
                        .createFolder("1_1_1");
            //2->2_1->2_1_1
            root1.createFolder("2")
                    .createFolder("2_1")
                        .createFolder("2_1_1");

            try {
                root1.getChildFolder("2")
                        .getChildFile("2_1")
                            .delete();
                throw new Error("Cannot delete not empty folder as file!");
            } catch (IOException ex) {
                //ok
            }

            root1.getChildFolder("2")
                    .getChildFolder("2_1")
                        .cascadeDelete();

            root1.getChildFolder("1")
                    .cascadeDelete();

            dump1 = root1.getView();
        }

        try (final FATFileSystem ffs = FATFileSystem.open(path)) {
            FATFolder root1 = ffs.getRoot();

            if (!dump1.equals(root1.getView()))
                throw new Error("Wrong dump!");
        }

        tearDown(path);
    }
    @Test
    public void testFileDelete() throws IOException {
        for (int allocatorType : allocatorTypes) {
            int clusterSize = FATFile.RECORD_SIZE*3/2; //fixed!
            int clusterCount = 20; //fixed!
            logStart(getPath(), clusterSize, clusterCount, allocatorType);
            testFileDelete(getPath(), clusterSize, clusterCount, allocatorType);
            logOk();
        }
    }

}
