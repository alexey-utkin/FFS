package com.test;

import org.junit.*;
import org.junit.rules.TestName;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Common test machinery.
 */
public class FATBaseTest {
    protected static FileSystem fs = FileSystems.getDefault();

    protected static final int[] allocatorTypes = new int[] {
            FATSystem.ALLOCATOR_CLASSIC_HEAP,
            FATSystem.ALLOCATOR_FAST_FORWARD};
    protected static final String[] allocationTypeNames = new String[] {
            "ClassicHeap",
            "FastForward"};

    protected static void log(String message) {
        System.out.print(message);
    }
    protected static void logLN(String message) {
        System.out.println(message);
    }
    static public void startUp(Path hostPath) throws IOException {
        Files.deleteIfExists(hostPath);
    }
    static public void tearDown(Path hostPath) throws IOException {
        //Files.deleteIfExists(hostPath);
    }
    static public void logStart(Path path, int clusterSize, int clusterCount,
                                int allocatorType)
    {
        log(    path + "/" + allocationTypeNames[allocatorType]
                + " cs=" + clusterSize
                + " cc=" + clusterCount
                + ":");
    }
    static public void logOk() {
        logLN("Ok");
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {
    }


    @Rule
    public TestName name = new TestName();

    public Path getPath() {
        return fs.getPath(name.getMethodName() + ".fs");
    }
}
