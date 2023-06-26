package simpledb;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final ConcurrentHashMap<PageId, Page> pages;

    // Adding the LockManager
    private final LockManager lockManager;

    private final Map<TransactionId, Long> systemRunTime;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>(numPages);
        this.lockManager = new LockManager();
        this.systemRunTime = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        // return null;

        // implementation of deadlock detection and resolution
        // We are using a Timeout Policy here
        long timeOut = System.currentTimeMillis();
        boolean acquired = lockManager.acquireLock(tid, pid, perm);
        if (!systemRunTime.containsKey(tid)) {
            systemRunTime.put(tid, timeOut);

            while(acquired){
                // I tweaked with different numbers to check how long the transaction system test takes
                // and this bound limit seemed the best to me
                if ((System.currentTimeMillis() - systemRunTime.get(tid)) > 50) {
                    throw new TransactionAbortedException();
                }
                try {
                    Thread.sleep(200);
                    acquired = lockManager.acquireLock(tid, pid, perm);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            } //end of while loop
        } else {
            // a bigger running transaction
            // I tweaked with different numbers to check how long the transaction system test takes
            // and this bound limit seemed the best to me
            while(acquired){
                if ((System.currentTimeMillis() - systemRunTime.get(tid)) > 100) {
                    throw new TransactionAbortedException();
                }
                try {
                    Thread.sleep(10);
                    acquired = lockManager.acquireLock(tid, pid, perm);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }//end of while loop
        }

        Page page = pages.get(pid);
        if (page != null) {
            return page;
        }

        // modified by Tasnim
        // now once page size is greater than or equal to numPages,
        // we go through the evictPage() function instead of just
        // straight throwing exception
        if (pages.size() >= numPages) {
            evictPage();
            // commented out previously thrown  exception in lab1
            // throw new DbException("The buffer pool is full!");
        }
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page newPage = dbFile.readPage(pid);
        pages.put(pid, newPage);

        return newPage;
    }
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseALock(tid,pid);
    }


    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        // return false;
        return lockManager.holdsLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */


    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // Iterate through all pages in the buffer pool

        // MODIFICATION BY TASNIM
        // THIS IS WHAT PRIMARILY CAUSING NOT TO PASS ANY OF THE TESTS :(

        for (Map.Entry<PageId, Page> pIDEntrySet : pages.entrySet()) {
            Page page = pIDEntrySet.getValue();

            // First we check whether our dirty page is null
            // and then the dirty page equals to given tid
            if (page.isDirty() != null && page.isDirty().equals(tid)) {

                // if we don't have a committed update
                // we put pages before getting the images
                if (!commit) {
                    pages.put(pIDEntrySet.getKey(), page.getBeforeImage());
                } else {
                    // instead of flushPage() call
                    Database.getLogFile().logWrite(tid,page.getBeforeImage(),page);
                    Database.getLogFile().force();

                    // use current page contents as the before-image
                    // for the next transaction that modifies this page.
                    page.setBeforeImage();
                    // finally call markDirty on our page
                    page.markDirty(false, null);
                }
            }
        }
        lockManager.releaseAll(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        if (dbFile instanceof HeapFile) {
            ArrayList<Page> affectedPages = ((HeapFile) dbFile).insertTuple(tid, t);
            for (Page page : affectedPages) {
                page.markDirty(true, tid);
                pages.put(page.getId(), page);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        if (dbFile instanceof HeapFile) {
            ArrayList<Page> affectedPages = ((HeapFile) dbFile).deleteTuple(tid, t);
            for (Page page : affectedPages) {
                page.markDirty(true, tid);
                pages.put(page.getId(), page);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

        // MADE MINOR CHANGES BY TASNIM SO PatchTest and TestFlushAll work
        for (PageId pagesID: pages.keySet()){
            flushPage(pagesID);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        // MINOR CHANGES MADE BY TASNIM TO MAKE TestFlushAll pass
        Page p = pages.get(pid);
        if (p != null && p.isDirty() != null) {
            // append an update record to the log, with a before-image and after-image
            TransactionId dirtier = p.isDirty();
            if (dirtier != null){
                Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
                Database.getLogFile().force();
            }  // If the dirtier is not running, it means it has already committed
               // So, we do not need to log the change again

            DbFile DbF = Database.getCatalog().getDatabaseFile(pid.getTableId());
            DbF.writePage(p);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        // DO WE NEED THIS METHOD? WHY IS IT HERE?

//        for (Map.Entry<PageId, Page> page : pages.entrySet()) {
//            Page p = page.getValue();
//
//            if (tid.equals(p.isDirty())) {
//                flushPage(page.getKey());
//                p.markDirty(false, null);
//                // Use current page contents as the before-image for the next
//                // transaction that modifies this page.
//                p.setBeforeImage();
//            }
//        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        for (Map.Entry<PageId, Page> pageIdPageEntry : pages.entrySet()) {
            PageId cleanPageId = pageIdPageEntry.getKey();
            if (pages.get(cleanPageId).isDirty() != null) {
                continue;
            }
            try {
                flushPage(cleanPageId);
            } catch (IOException e) {
                throw new DbException(e.getMessage());
            }
            discardPage(cleanPageId);
            return;
        }
        throw new DbException("All pages in the buffer pool are dirty and cannot be evicted");
    } // changed

}