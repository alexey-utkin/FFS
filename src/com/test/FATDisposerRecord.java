package com.test;

/**
 * The interface for [dispose] action on GC call.
 *
 * Implementation:
 * @see FATFolder
 * @see FATFile
 */

public interface FATDisposerRecord {
    public void dispose();
}
