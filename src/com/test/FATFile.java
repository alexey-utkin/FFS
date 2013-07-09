package com.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides File object functionality as Attributes and Content storage.
 * @see FATFileChannel
 * @see FATFolder
 *
 *
 * All [transact-safe] methods have the [ts_] prefix.
 * The [transact-safe] means that methods can terminated successfully,
 *   or restore the object state to initial condition and throw exception,
 *   or mark system as "dirty" and throw exception (critical error in host FS).
 *
 * All public functions have to be [transact-safe] by default.
 */
public class FATFile {
    public static final int INVALID_FILE_ID = -1;
    public static final int ROOT_FILE_ID = 0;
    public static final int FILE_MAX_NAME = 110;
    public static final char ZAP_CHAR = 0xCDCD;

    public static final int TYPE_FILE = 0;
    public static final int TYPE_FOLDER = 1;
    public static final int TYPE_DELETED = -1;

    static final FATFile DELETED_FILE = new FATFile(null, null, INVALID_FILE_ID, TYPE_DELETED);

    final FATFileSystem fs;

    public static final int RECORD_NAME_OFFSET = 3*4 + 3*8;
    public static final int RECORD_SIZE = RECORD_NAME_OFFSET + FILE_MAX_NAME*2;  //256 bytes

    //RW Lock
    private final ReentrantReadWriteLock lockRW = new ReentrantReadWriteLock();

    // attributes
    private final int type;
    private int fileId = INVALID_FILE_ID;
    private int  access;
    private long size;
    private long timeCreate;
    private long timeModify;
    private final char[] name = new char[FILE_MAX_NAME];
    private boolean initialized;
    private boolean isFrozen = false;

    //PERFORMANCE HINT: bad
    //hard link to parent
    private FATFile fatParent; 

    void checkSelfValid() throws IOException {
        if (fileId == INVALID_FILE_ID)
            throw new IOException("Invalid file id.");
    }

    void checkParent() throws IOException {
        if (isRoot())
            return;
        if (fatParent == null)
            throw new IOException("Invalid parent ref.");
        fatParent.checkSelfValid();
    }
    
    void checkValid() throws IOException {
        checkSelfValid();
        checkParent();
    }


    FATFileChannel getChannelInternal(boolean appendMode) throws IOException {
        return new FATFileChannel(this, appendMode);
    }

    /**
     * Opens file channel for file context access.
     * @param appendMode if [true] the [write] call always add the buffer to the file tail
     * @return the channel for context read/write operations.
     * @throws IOException
     * @throws FATFileLockedException
     */
    public FATFileChannel getChannel(boolean appendMode) throws IOException {
        if (isFolder())
            throw new IOException("That is a folder.");
        return getChannelInternal(appendMode);
    }

    /**
     * Rename the file, if can
     *
     * @throws IOException
     * @throws FATFileLockedException
     */
    public void rename(String newName) throws IOException {
        FATLock lock = tryLockThrowInternal(true);
        try {
            if (isRoot())
                throw new IOException("Cannot rename root.");
            getParent().ts_renameChild(this, newName);
            //commit
        } finally {
            lock.unlock();
        }
    }

    /**
     * Deletes the file, if it is not locked
     *
     * @throws IOException
     * @throws FATFileLockedException
     */
    public void delete() throws IOException {
        FATLock lock = tryLockThrowInternal(true);
        try {
            if (isFolder() && !isEmpty())
                throw new DirectoryNotEmptyException(getName());
            if (isRoot())
                throw new IOException("Cannot delete root.");
            getParent().ts_deRef(this);
            fs.ts_dropDirtyFile(this);
            //commit
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() throws IOException {
        return length() == 0;
    }

    FATFile ts_rl_getParentAsFile() {
        return isRoot()
                ? null
                : fatParent;
    }

    public FATFolder getParent() throws IOException {
        FATLock lock = tryLockThrowInternal(false);
        try {
            return isRoot()
                ? null
                : fatParent.getFolder();
        } finally {
            //dirty rollback
            lock.unlock();
        }
    }

    public void force(boolean updateMetadata) throws IOException {
        FATLock lock = tryLockThrowInternal(true);
        try {
            if (updateMetadata)
                updateLastModified();
            fs.ts_forceFileContent(this, updateMetadata);
        } finally {
            //dirty rollback
            lock.unlock();
        }
    }

    public FATFolder getFolder() throws IOException {
        FATLock lock = tryLockThrowInternal(false);
        try {
            if (!isFolder())
                return null;
            FATFolder ret = fs.ts_getFolderFromCache(fileId);
            if (ret == null) {
                //ts_ constructor
                ret = new FATFolder(this);
                ret.ts_rl_readContent();
            }
            return ret;
        } finally {
            //dirty rollback
            lock.unlock();
        }
    }

    /**
     * Moves file to new location if can.
     *
     * @param newParent new owner of the file
     * @throws IOException
     */
    public void moveTo(FATFolder newParent) throws IOException {
        if (newParent == null)
            throw new IllegalArgumentException("Bad new parent");
        if (isRoot())
            throw new IOException("Cannot move the root.");

        //only one move or delete at once
        freeze();
        try {
            final FATFile oldParentFile = fatParent;
            //cannot run away from deleting folder
            oldParentFile.freeze();
            try {
                int nestUp = 0;
                final FATFile newParentFile = newParent.asFile();
                try {
                    //lock chain from move for [newParent]
                    FATFile newUpParentFile = newParentFile;
                    while (newUpParentFile != null) {
                        if (!newUpParentFile.tryToFreeze()) {
                            if (newParentFile.fileId == oldParentFile.fileId)
                                return;//nothing to do
                            if (newUpParentFile.fileId == fileId)
                                throw new IOException("Cannot move to child or self.");
                            throw new FATFileLockedException(newUpParentFile,true);
                        }
                        ++nestUp;
                        newUpParentFile = newUpParentFile.fatParent;
                    }

                    //up lock order
                    //newParentFile <?- oldParentFile <- this

                    FATLock lockNewParentFile = newParentFile.getLockInternal(true);
                    try {
                        FATLock lockOldParentFile = oldParentFile.getLockInternal(true);
                        try {
                            //don't need [this] lock - it's protected from [move] or [delete]

                            //PERFORMANCE HIT
                            // Reserve storage first.
                            // Yes, I do not support move on partition without space.
                            // Else I need to take a storage lock. That is dramatically
                            // reduce parallel operations.
                            newParent.ts_wl_reserveRecord();

                            // storage have a space for new record.
                            // since now - no way back - maintenance mode only
                            boolean success = false;
                            try {
                                getParent().ts_deRef(this); //oldParentFile locked
                                newParent.ts_wl_ref(this); // newParentFile locked
                                //here is the only place where parent is been changing
                                fatParent = newParentFile;
                                success = true;
                            } finally {
                                if (!success)
                                    fs.ts_setDirtyState("Cannot rollback movement of the file. ", false);
                            }
                        } finally {
                            lockOldParentFile.unlock();
                        }
                    } finally {
                        lockNewParentFile.unlock();
                    }
                } finally {
                    //unlock chain from move for [newParent]
                    FATFile newUpParentFile = newParentFile;
                    while (newUpParentFile != null && nestUp > 0) {
                        newUpParentFile.unfreeze();
                        --nestUp;
                        newUpParentFile = newUpParentFile.fatParent;
                    }
                }

            } finally {
                oldParentFile.unfreeze();
            }
        } finally {
            unfreeze();
        }
    }

    synchronized boolean isFrozen() {
        return isFrozen;
    }

    synchronized boolean tryToFreeze() {
        if (isFrozen)
            return false;
        isFrozen = true;
        return true;
    }

    synchronized void freeze() throws FATFileLockedException {
        if (isFrozen)
            throw new FATFileLockedException(this, true);
        isFrozen = true;
    }

    synchronized void unfreeze() {
        if (!isFrozen)
            System.err.println("Bad State");
        isFrozen = false;
    }



    static String unlockedGetName(char[] name) {
        String ret = new String(name);
        int zeroPos = ret.indexOf(ZAP_CHAR);
        return (zeroPos == -1)
                ? ret
                : ret.substring(0, zeroPos);
    }

    public String getName() throws IOException {
        FATLock lock = tryLockThrowInternal(false);
        try {
            return unlockedGetName(name);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return unlockedGetName(name) + "[" + super.toString() + "]";
    }

    /**
     * Gets the file length.
     *
     * @return the file length
     */
    public long length() throws IOException {
        FATLock lock = tryLockThrowInternal(false);
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    void setLengthInternal(long newLength) throws IOException {
        FATLock lock = tryLockThrowInternal(true);
        try {
            if (newLength == size)
                return;
            fs.setFileLength(this, newLength, size);
            size = newLength;
            // commit
            ts_wl_updateAttributes(); //no rollback - [dirty]
        } finally {
            lock.unlock();
        }
    }
    /**
     * Sets the length of this file.
     *
     * If the present length of the file as returned by the length method is
     * greater than the newLength argument then the file will be truncated.
     *
     * If the present length of the file as returned by the length method
     * is smaller than the newLength argument then the file will be extended.
     * In this case, the contents of the extended portion of the file are not defined.
     *
     * @param newLength The desired length of the file
     */
    public void setLength(long newLength) throws IOException {
        if (isFolder())
            throw new IOException("That is a folder.");
        setLengthInternal(newLength);
    }


    /**
     * Tests whether the file denoted by this is a directory.
     *
     * Checks the file type against [TYPE_FOLDER] const.
     *
     * @return [true] if and only if the file denoted by this
     *         exists and is a directory; [false] otherwise
     */
    public boolean isFolder() {
        //fast safe check for final field
        return type == TYPE_FOLDER;
    }

    /**
     * Tests whether the file denoted by this is a normal file.
     *
     * Checks the file type against [TYPE_FILE] const.
     *
     * @return  [true] if and only if the file denoted by this exists and
     *          is a normal file; [false] otherwise
     */
    public boolean isFile()  {
        //fast safe check for final field
        return type == TYPE_FILE;
    }

    /**
     * Gets the file access attribute.
     *
     * @return the file access state.
     */
    public int access() throws IOException {
        FATLock lock = tryLockThrowInternal(false);
        try {
            return access;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sets the file access attribute.
     *
     * @param access the file access state.
     */
    public void setAccess(int access) throws IOException {
        FATLock lock = tryLockThrowInternal(true);
        try {
            if (this.access == access)
                return;
            this.access = access;
            // commit
            ts_wl_updateAttributes(); //no rollback - [dirty]
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets the file creation time attribute.
     *
     * @return the file creation time in milliseconds.
     */
    public long timeCreate() throws IOException {
        FATLock lock = tryLockThrowInternal(false);
        try {
            return timeCreate;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sets the file creation time attribute.
     *
     * @param timeCreate the file creation time in milliseconds.
     */
    public void setTimeCreate(long timeCreate) throws IOException {
        FATLock lock = tryLockThrowInternal(true);
        try {
            if (this.timeCreate == timeCreate)
                return;

            this.timeCreate = timeCreate;
            // commit
            ts_wl_updateAttributes(); //no rollback - [dirty]
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets the file modification time attribute.
     *
     * @return the file modification time in milliseconds.
     */
    public long lastModified() throws IOException {
        FATLock lock = tryLockThrowInternal(false);
        try {
            return timeModify;
        } finally {
            lock.unlock();
        }
    }


    public void updateLastModified() throws IOException {
        setLastModified(FATFileSystem.getCurrentTime());
    }

    /**
     * Sets the file modification time attribute.
     *
     * @param timeModify the file modification time in milliseconds.
     */
    public void setLastModified(long timeModify) throws IOException {
        FATLock lock = tryLockThrowInternal(true);
        try {
            if (this.timeModify == timeModify)
                return;

            this.timeModify = timeModify;
            // commit
            ts_wl_updateAttributes(); //no rollback - [dirty]
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check the root status for file.
     *
     * @return [true] if it is a root file.
     */
    public boolean isRoot() {
        //fast safe check for final field
        return (fileId == ROOT_FILE_ID);
    }


    /**
     * Creates file from id (only for DELETED & ROOT) in dirty state
     *
     * @param fs the FS object
     * @param fileId the FS unique file Id (in FATSystem that is the start of file chain)
     * @param type the [TYPE_XXXX] const
     */
    FATFile(FATFileSystem fs, FATFile parent, int fileId, int type) {
        this.fs = fs;
        // both ids validated in upper calls
        this.fileId = fileId;
        this.type = type;
        this.fatParent = parent;
        initialized = false;
        if (type == TYPE_DELETED) {
            Arrays.fill(name, (char)0xFFFF);
        } else if (type == TYPE_FOLDER && fileId == ROOT_FILE_ID){
            initName(FATFolder.ROOT_NAME);
            fs.addFile(this, new SelfDisposer(fs, fileId));
        }
    }

    /**
     * Creates File in FAT with allocated space.
     *
     * Could not be called directly, use [fs.ts_createFile] instead.
     *
     * @param parent the parent object
     * @param name the name of created file
     * @param type the [TYPE_XXXX] const
     * @param size the size of created file, that need to be allocated
     * @param access the desired access
     * @throws IOException
     */
    FATFile(FATFolder parent, String name, int type, long size, int access) throws IOException {
        initName(name);
        this.fs = parent.fatFile.fs;
        this.fatParent = parent.fatFile; 
        fileId = fs.ts_allocateFileSpace(size);
        this.type = type;
        this.size = size;
        timeCreate = FATFileSystem.getCurrentTime();
        timeModify = FATFileSystem.getCurrentTime();
        this.access = access;
        initialized = true;
        fs.addFile(this, new SelfDisposer(fs, fileId));
    }

    void ts_initFromBuffer(ByteBuffer bf) throws IOException {
        // [fileId] and [type] was read before for [ctr] call
        if (!initialized) {
            size = bf.getLong();
            timeCreate = bf.getLong();
            timeModify = bf.getLong();
            access = bf.getInt();
            // only UNICODE name for performance and compatibility reasons
            bf.asCharBuffer().get(name);
            //file holds actual value => no more updates from parent stream.
            initialized = true;
        } else {
            fs.ts_setDirtyState("Double init for file:" + fileId, true);
        }
    }

    ByteBuffer ts_serialize(ByteBuffer bf, int version) {
        bf
                .putInt(fileId)
                .putInt(type)
                .putLong(size)
                .putLong(timeCreate)
                .putLong(timeModify)
                .putInt(access);
        // only UNICODE name for performance and compatibility reasons
        bf.asCharBuffer().put(name);
        bf.position(bf.position() + name.length*2);
        return bf;
    }

    /**
     * Updates attribute info in parent record if any.
     *
     * Have to be called under [fileLock].
     * @throws IOException
     */
    private void ts_wl_updateAttributes() throws IOException {
        //file holds actual value => no more updates from parent stream.
        initialized = true;
        if (isRoot())
            fs.ts_updateRootFileRecord(this);
        else if (fatParent != null) {
            getParent().ts_updateFileRecord(this);
        }
    }

    void initName(String fileName) {
        int len = fileName.length();
        if (len > FILE_MAX_NAME)
            throw new IllegalArgumentException("Name is too long. Max length is " + FILE_MAX_NAME);

        Arrays.fill(name, ZAP_CHAR);
        System.arraycopy(fileName.toCharArray(), 0, name, 0, len);
        //no update here! That is init!
    }

    int ts_getFileId() {
        return fileId;
    }

    void ts_setFileId(int fileId) {
        this.fileId = fileId;
    }

    public int getType() {
        return type;
    }

    void ts_initNewRoot(long size, int access) throws IOException {
        if (!isRoot() || initialized)
            fs.ts_setDirtyState("Wrong root init call", true);

        fileId = fs.ts_allocateFileSpace(FATFolder.EMPTY_FILE_SIZE);
        if (!isRoot())
            fs.ts_setDirtyState("Root already exists", true);

        initName(FATFolder.ROOT_NAME);
        this.size = size;
        timeCreate = FATFileSystem.getCurrentTime();
        timeModify = FATFileSystem.getCurrentTime();
        this.access = access;
        initialized = true;
        fs.ts_updateRootFileRecord(this);
    }

    private static class SelfDisposer implements FATDisposerRecord {
        private final FATFileSystem fs;        
        private final int fileId;
        SelfDisposer(FATFileSystem fs, int fileId) {
            this.fs = fs;
            this.fileId = fileId;
        }
        @Override 
        public void dispose() {
            fs.disposeFile(fileId);
        }
    }

    private FATLock getFATLockAndCheck(FATFileSystem fs, Lock lock) throws IOException {
        FATLock ret = new FATLock(fs, lock);
        boolean success = false;
        try {
            checkValid();
            success = true;
        } finally {
            if (!success) {
                ret.unlock();
            }
        }
        return ret;
    }

    FATLock getLockInternal(boolean write) throws IOException {
        fs.begin(write);
        Lock lock = write
                ? lockRW.writeLock()
                : lockRW.readLock();
        lock.lock();
        return getFATLockAndCheck(fs, lock);
    }
    /**
     * Locks the file.
     *
     * Returns the lock that need to unlocked.
     *
     * @param write [true] - locks file for write, [false] - for read operations
     * @return the FAT lock with enclosed transaction.
     * @throws IOException
     */
    public FATLock getLock(boolean write) throws IOException {
        if (isFolder())
            throw new IOException("That is a folder.");
        return getLockInternal(write);
    }


    FATLock tryLockThrowInternal(boolean write) throws IOException {
        fs.begin(write);
        Lock lock = write
                ? lockRW.writeLock()
                : lockRW.readLock();
        if (!lock.tryLock()) {
            fs.end();
            throw new FATFileLockedException(this, write);
        }
        return getFATLockAndCheck(fs, lock);
    }
    /**
     * Locks the file if possible, throws [FATFileLockedException] if cannot.
     *
     * Returns the lock that need to unlocked, or throws [FATFileLockedException].
     *
     * @param write  [true] - locks file for write, [false] - for read operations
     * @return the FAT lock with enclosed transaction.
     * @throws IOException
     */
    public FATLock tryLockThrow(boolean write) throws IOException {
        if (isFolder())
            throw new IOException("That is a folder.");
        return tryLockThrowInternal(write);
    }
}
