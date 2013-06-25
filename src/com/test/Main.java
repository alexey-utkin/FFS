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
                    FATSystem.testCreate(testFFS, FolderEntry.RECORD_SIZE, clusterCount);
                    System.out.println("testCreate FS Size: " + clusterCount + " OK.");
                }
            }

            {
                int[] clusterCounts = new int[] {
                        16, 32,
                        (int)(0x80000000L/FolderEntry.RECORD_SIZE)
                        //(int)(0x800000000L/FolderEntry.RECORD_SIZE)
                };
                for (int clusterCount : clusterCounts) {
                    FATSystem.testCriticalFatAllocation(testFFS, FolderEntry.RECORD_SIZE, clusterCount);
                    System.out.println("testCriticalFatAllocation FS Size: " + clusterCount + " OK.");
                }
            }
*/
            {
                int[] clusterCounts = new int[] {
                        16, 32, 64
                };
                for (int clusterCount : clusterCounts) {
                    FATSystem.testConcurrentFragmentation(testFFS, FolderEntry.RECORD_SIZE, clusterCount);
                    System.out.println("testConcurrentFragmentation FS Size: " + clusterCount + " OK.");
                }
            }

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
