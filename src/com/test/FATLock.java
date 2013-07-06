package com.test;

import java.util.concurrent.locks.Lock;

/**
 * Transaction terminator with the lock
 */
class FATLock {
    private final Lock lock;
    private final FATFileSystem fs;

    public FATLock(FATFileSystem fs, Lock lock) {
        this.lock = lock;
        this.fs = fs;
    }

    public void unlock() {
        lock.unlock();
        fs.end();
    }

}
