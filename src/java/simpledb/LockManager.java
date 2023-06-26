package simpledb;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


public class LockManager {
    private final Map<TransactionId, Set<PageId>> sharedLock;
    private final Map<TransactionId, Set<PageId>> exclusiveLock;
    private final Map<PageId, Set<TransactionId>> readLock;
    private final Map<PageId, TransactionId> writeLock;


    public LockManager() {
        sharedLock = new ConcurrentHashMap<>();
        exclusiveLock = new ConcurrentHashMap<>();
        readLock = new ConcurrentHashMap<>();
        writeLock = new ConcurrentHashMap<>();
    }

    /**
     * Based on the permission value (whether it is READ_ONLY or not
     * We make sure which lock to call
     * @param tid
     * @param pid
     * @param perm
     * @return acquireSharedLock if the permission is READ_ONLY
     * else returns acquireExclusiveLock
     */
    public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) {
        if (perm.equals(Permissions.READ_ONLY)) {
            return acquireSharedLock(tid, pid);
        } else {
            return acquireExclusiveLock(tid, pid);
        }
    }

    /**
     * allows multiple transactions to read the same page concurrently
     * without interfering with each other
     * by acquiring a shared lock on a page for a given transaction
     * @param tid
     * @param pid
     * @return false if there is writeLock on a transaction
     * otherwise returns true
     */
    public synchronized boolean acquireSharedLock(TransactionId tid, PageId pid) {
        Set<TransactionId> readL = readLock.get(pid);
        if (writeLock.get(pid) == null || writeLock.get(pid).equals(tid)) {
            if (readL == null) {
                readL = new HashSet<>();
            }
            readL.add(tid);
            readLock.put(pid, readL);

            Set<PageId> shareL = sharedLock.get(tid);
            if (shareL == null) {
                shareL = new HashSet<>();
            }
            shareL.add(pid);
            sharedLock.put(tid, shareL);
            return false;
        }
        return true;
    }

    /**
     * allows an exclusive lock in a multi-threaded or concurrent environment
     * @param tid
     * @param pid
     * @return true when
     * 1. readL is not null and size is 1 and it contains one transaction id
     * 2. readL has more than one ransactions
     * 3. writeLock is not null & writeLocks pid is equal to tid
     * else there is a false return based on more case analysis
     */
    public synchronized boolean acquireExclusiveLock(TransactionId tid, PageId pid) {
        Set<TransactionId> readL = readLock.get(pid);
        if (readL != null && readL.size() == 1 && !readL.contains(tid)) {
            return true;
        }
        if (readL != null && readL.size() > 1) {
            return true;
        }
        if (writeLock.get(pid) != null && !writeLock.get(pid).equals(tid)) {
            return true;
        } else {
            writeLock.put(pid, tid);
            Set<PageId> exclusiveL = exclusiveLock.get(tid);
            if (exclusiveL == null) {
                exclusiveL = new HashSet<>();
            }
            exclusiveL.add(pid);
            exclusiveLock.put(tid, exclusiveL);

            return false;

        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     *
     * @param tid the ID of the transaction requesting the hold
     * @param pid the ID of the page to hold
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return (sharedLock.containsKey(tid) && sharedLock.get(tid).contains(pid))
                || (exclusiveLock.containsKey(tid) && exclusiveLock.get(tid).contains(pid));
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     * We call it through lockManager in BufferPool
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releaseALock(TransactionId tid, PageId pid) {
        Set<TransactionId> readL = readLock.get(pid);
        Set<PageId> shareLock = sharedLock.get(tid);
        Set<PageId> exclusiveL = exclusiveLock.get(tid);
        if (readL != null) {
            readL.remove(tid);
            readLock.put(pid, readL);
        }
        if (shareLock != null) {
            shareLock.remove(pid);
            sharedLock.put(tid, shareLock);
        }
        if (exclusiveL != null) {
            exclusiveL.remove(pid);
            exclusiveLock.put(tid, exclusiveL);
        }
        writeLock.remove(pid);
    }

    /**
     * To release all the transaction locks when needed
     * @param tid
     */
    public synchronized void releaseAll(TransactionId tid) {
        for (PageId pid : writeLock.keySet()) {
            if (writeLock.get(pid) == tid && writeLock.get(pid) != null) {
                writeLock.remove(pid);
            }
        }
        exclusiveLock.remove(tid);

        for (PageId pageId : readLock.keySet()) {
            Set<TransactionId> readL = readLock.get(pageId);
            if (readL != null) {
                readL.remove(tid);
                readLock.put(pageId, readL);

            }
        }
        sharedLock.remove(tid);
    }
}





