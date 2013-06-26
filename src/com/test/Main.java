package com.test;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {
        try {
            FileSystem fs = FileSystems.getDefault();
            Path testFFS = fs.getPath("createTest.fs");


            {
                int[] clusterCounts = new int[] {
                        16, 32,
                        //(int)(0x800000000L/FolderEntry.RECORD_SIZE)
                };
                for (int clusterCount : clusterCounts) {
                    System.out.print("testCreate FS Size " + clusterCount + ":");
                    FATSystemTests.testCreate(fs.getPath("testCreate.fs"),
                        FolderEntry.RECORD_SIZE,
                        clusterCount);
                    System.out.println("OK");
                }
            }

            {
                int[] clusterCounts = new int[] {
                        16, 32,
                        //(int)(0x80000000L/FolderEntry.RECORD_SIZE)
                };
                for (int clusterCount : clusterCounts) {
                    System.out.print("testCriticalFatAllocation FS Size " + clusterCount + ":");
                    FATSystemTests.testCriticalFatAllocation(fs.getPath("testCriticalFatAllocation.fs"),
                        FolderEntry.RECORD_SIZE,
                        clusterCount);
                    System.out.println("OK");
                }
            }
            {
                // size must be > 30
                int[] clusterCounts = new int[] {
                        31, 1024, 4096
                };
                for (int clusterCount : clusterCounts) {
                    System.out.print("testConcurrentFragmentation FS Size " + clusterCount + ":");
                    FATSystemTests.testConcurrentFragmentation(fs.getPath("testConcurrentFragmentation.fs"),
                            FolderEntry.RECORD_SIZE,
                            clusterCount);

                    System.out.println("OK");
                }
            }

            {
                int clusterCount = 16;// > 12
                System.out.print("testConcurrentSafeClose FS Size " + clusterCount + ":");
                FATSystemTests.testConcurrentSafeClose(fs.getPath("testConcurrentFragmentation.fs"),
                        FolderEntry.RECORD_SIZE,
                        clusterCount);
                System.out.println("OK");
            }


        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
