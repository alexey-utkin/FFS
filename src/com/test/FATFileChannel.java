package com.test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Provides access to Content storage.
 * @see FATFile
 */

public class FATFileChannel implements Closeable {
    //hash map on start cluster for exclusive access
    private final FATFile fatFile;
    private long position;
    private final boolean appendMode;

    /**
     * Returns the channel to work with file content
     *
     * @param file the FAT file for read-write operations
     * @param appendMode [true] means, that position moves to
     *                   the end of file right before {@link #write(java.nio.ByteBuffer)}
     *                   operation. Look into testLostWrite test.
     */
    public FATFileChannel(FATFile file, boolean appendMode) {
        fatFile = file;
        this.appendMode = appendMode;
        position = 0;
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     * 
     * <p> Bytes are read starting at this channel's current file position, and
     * then the file position is updated with the number of bytes actually
     * read.  Otherwise this method behaves exactly as specified in the {@link
     * java.nio.channels.ReadableByteChannel} interface.
     *
     * @throws java.io.IOException If some other I/O error occurs
     */
    public int read(ByteBuffer dst) throws IOException {
        FATLock lock = fatFile.tryLockThrowInternal(false);
        try {
            synchronized (this) { //protect the position
                long rest = fatFile.length() - position;
                if (rest <= 0)
                    return -1;

                //we need to protect from read-after-EOF
                if (dst.remaining() > rest)
                    dst.limit((int) (dst.position() + rest));

                int wasRead = fs().readFileContext(fatFile, position, dst);
                // commit
                position += wasRead;
                return wasRead;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     * 
     * <p> Bytes are written starting at this channel's current file position
     * unless the channel is in append mode, in which case the position is
     * first advanced to the end of the file.  The file is grown, if necessary,
     * to accommodate the written bytes, and then the file position is updated
     * with the number of bytes actually written.  Otherwise this method
     * behaves exactly as specified by the {@link java.nio.channels.WritableByteChannel}
     * interface.
     *
     * @throws java.io.IOException If some other I/O error occurs
     */
    public int write(ByteBuffer src) throws IOException {
        long sizeToWrite = src.limit() - src.position();
        if (sizeToWrite < 0) //Nothing to do
            return 0;

        int wasWritten = 0;
        //Lock Attribute due to file size change
        FATLock lock = fatFile.tryLockThrowInternal(true);
        try {
            synchronized (this) { //protect the position
                if (appendMode)
                    position = fatFile.length();
                long finalPos = position + sizeToWrite;
                boolean success = false;
                if (finalPos > fatFile.length())
                    fatFile.setLengthInternal(finalPos);
                else
                    success = true; //no rollback
                try {
                    wasWritten = fs().writeFileContext(fatFile, position, src);
                    if (wasWritten != sizeToWrite) {
                        // sbj for adjustment in NFS
                        throw new IOException("Chanel write error");
                    }
                    // commit
                    success = true;
                    position += wasWritten;
                } finally {
                    if (!success) {
                        // rollback
                        fatFile.setLengthInternal(finalPos);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        return wasWritten;
    }


    /**
     * Returns this channel's file position.
     *
     * @return This channel's file position,
     *         a non-negative integer counting the number of bytes
     *         from the beginning of the file to the current position
     */
    public long position() {
        synchronized (this) { //protect the position
            return position;
        }
    }

    /**
     * Sets this channel's file position.
     * 
     * <p> Setting the position to a value that is greater than the file's
     * current size is legal but does not change the size of the file.  A later
     * attempt to read bytes at such a position will immediately return an
     * end-of-file indication.  A later attempt to write bytes at such a
     * position will cause the file to be grown to accommodate the new bytes;
     * the values of any bytes between the previous end-of-file and the
     * newly-written bytes are unspecified.  
     *
     * @param newPosition The new position, a non-negative integer counting
     *                    the number of bytes from the beginning of the file
     * @return This file channel
     * @throws java.io.IOException      If some other I/O error occurs
     */
    public FATFileChannel position(long newPosition) throws IOException {
        if (newPosition < 0)
            throw new IOException("Bad new position");
        synchronized (this) {
            position = newPosition;
        }
        return this;
    }

    /**
     * Returns the current size of this channel's file.
     * 
     * @return The current size of this channel's file,
     *         measured in bytes
     * @throws java.nio.channels.ClosedChannelException
     *                             If this channel is closed
     * @throws java.io.IOException If some other I/O error occurs
     */
    public long size() throws IOException {
        return fatFile.length();
    }

    /**
     * Truncates this channel's file to the given size.
     * 
     * <p> If the given size is less than the file's current size then the file
     * is truncated, discarding any bytes beyond the new end of the file.  If
     * the given size is greater than or equal to the file's current size then
     * the file is not modified.  In either case, if this channel's file
     * position is greater than the given size then it is set to that size.
     * 
     * @param size The new size, a non-negative byte count
     * @return This file channel
     * @throws java.io.IOException If some other I/O error occurs
     */
    public FATFileChannel truncate(long size) throws IOException {
        synchronized (this) {
            if (size < size()) {
                fatFile.setLengthInternal(size);
                position = Math.max(position, size);
            }
        }
        return this;
    }

    /**
     * Forces any updates to this channel's file to be written to the storage
     * device that contains it.
     * 
     * <p> If this channel's file resides on a local storage device then when
     * this method returns it is guaranteed that all changes made to the file
     * since this channel was created, or since this method was last invoked,
     * will have been written to that device.  This is useful for ensuring that
     * critical information is not lost in the event of a system crash.
     * 
     * <p> If the file does not reside on a local device then no such guarantee
     * is made.
     * 
     * <p> The <tt>metaData</tt> parameter can be used to limit the number of
     * I/O operations that this method is required to perform.  Passing
     * <tt>false</tt> for this parameter indicates that only updates to the
     * file's content need be written to storage; passing <tt>true</tt>
     * indicates that updates to both the file's content and metadata must be
     * written, which generally requires at least one more I/O operation.
     * Whether this parameter actually has any effect is dependent upon the
     * underlying operating system and is therefore unspecified.
     * 
     * <p> Invoking this method may cause an I/O operation to occur even if the
     * channel was only opened for reading.  Some operating systems, for
     * example, maintain a last-access time as part of a file's metadata, and
     * this time is updated whenever the file is read.  Whether or not this is
     * actually done is system-dependent and is therefore unspecified.
     * 
     *
     * @param metaData If <tt>true</tt> then this method is required to force changes
     *                 to both the file's content and metadata to be written to
     *                 storage; otherwise, it need only force content changes to be
     *                 written
     * @throws java.io.IOException If some other I/O error occurs
     */
    public void force(boolean metaData) throws IOException {
        if (metaData) {
            fatFile.updateLastModified();
        }
        fatFile.force(metaData);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     * 
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        //force(false);
    }

    private FATFileSystem fs() {
        return fatFile.fs;
    }
}
