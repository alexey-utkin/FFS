package com.test;

import java.io.IOException;

/**
 * Failed lock attempt exception
 */
public class FATFileLockedException extends IOException {
    final private FATFile file;
    final private boolean write;

    public FATFileLockedException(FATFile file, boolean write) {
        this.file = file;
        this.write = write;
    }

    public FATFile getFile() {return file;}
    public boolean isWrite() {return write;}
}
