package com.test;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is used for registering and disposing.
 *
 * @see FATDisposerRecord
 */

public final class FATDisposer implements Runnable {
    private static final ReferenceQueue queue = new ReferenceQueue();
    private static final FATDisposer disposerInstance = new FATDisposer();
    private static final Set<WeakDisposerRecord> records = new HashSet<>();

    static {
        Thread t = new Thread(disposerInstance, "FATDisposer");
        t.setDaemon(true);
        t.setPriority(Thread.MAX_PRIORITY);
        t.start();
    }

    /**
     * Registers the object and the native data for later disposal.
     *
     * @param target Object to be registered
     * @param rec the associated FATDisposerRecord object
     * @see FATDisposerRecord
     */
    public static void addRecord(Object target, FATDisposerRecord rec) {
        disposerInstance.add(target, rec);
    }

    /**
     * Performs the actual registration of the target object to be disposed.
     *
     * @param target Object to be registered
     * @param rec the associated FATDisposerRecord object
     * @see FATDisposerRecord
     */
    private synchronized void add(Object target, FATDisposerRecord rec) {
        records.add(new WeakDisposerRecord(target, rec));
    }

    @Override
    public void run() {
        while (true) {
            try {
                WeakDisposerRecord obj = (WeakDisposerRecord) queue.remove();
                obj.clear();
                obj.dispose();
            } catch (Exception e) {
                System.out.println("Exception while removing reference: " + e);
                e.printStackTrace();
            }
        }
    }

    public static class WeakDisposerRecord
        extends WeakReference
        implements FATDisposerRecord
    {
        protected WeakDisposerRecord(Object referent) {
            super(referent, FATDisposer.queue);
            this.record = null;
        }

        private WeakDisposerRecord(Object referent, FATDisposerRecord record) {
            super(referent, FATDisposer.queue);
            this.record = record;
        }

        private final FATDisposerRecord record;

        @Override
        public void dispose() {
            record.dispose();
        }
    }
}
