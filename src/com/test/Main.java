package com.test;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {
        try {
            FileSystem fs = FileSystems.getDefault();
            Path testFFS = fs.getPath("createTest.fs");

/*
            {
                int[] clusterCounts = new int[] {
                        16, 32,
                        (int)(0x800000000L/FolderEntry.RECORD_SIZE)
                };
                for (int clusterCount : clusterCounts) {
                    System.out.print("testCreate FS Size " + clusterCount + ":");
                    FATSystemTests.testCreate(testFFS, FolderEntry.RECORD_SIZE, clusterCount);
                    System.out.println("OK");
                }
            }

            {
                int[] clusterCounts = new int[] {
                        16, 32,
                        (int)(0x80000000L/FolderEntry.RECORD_SIZE)
                        //(int)(0x800000000L/FolderEntry.RECORD_SIZE)
                };
                for (int clusterCount : clusterCounts) {
                    System.out.print("testCriticalFatAllocation FS Size " + clusterCount + ":");
                    FATSystemTests.testCriticalFatAllocation(testFFS, FolderEntry.RECORD_SIZE, clusterCount);
                    System.out.println("OK");
                }
            }
*/
            {
                // size must be > 30
                int[] clusterCounts = new int[] {
                        31, 1024, 4096
                };
                for (int clusterCount : clusterCounts) {
                    System.out.print("testConcurrentFragmentation FS Size " + clusterCount + ":");
                    FATSystemTests.testConcurrentFragmentation(testFFS, FolderEntry.RECORD_SIZE, clusterCount);
                    System.out.println("OK");
                }
            }

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
