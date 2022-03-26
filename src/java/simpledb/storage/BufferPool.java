package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
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

    private ConcurrentHashMap<PageId, Page> pageCache;
    private int numPages;
    private LockManager lock;

    private class LockManager {
        private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Permissions>> lock;

        LockManager() {
            lock = new ConcurrentHashMap<>();
        }

        public synchronized boolean acquireLock(PageId pid, TransactionId tid, Permissions perm) {
            if (!lock.containsKey(pid)) {
                lock.put(pid, new ConcurrentHashMap<>());
            }

            boolean acquired = false;
            TransactionId writeLockTid = null;
            int readLockCount = 0;
            boolean haveReadLock = false;

            for (Map.Entry<TransactionId, Permissions> e : lock.get(pid).entrySet()) {
                if (e.getValue() == Permissions.READ_ONLY) {
                    readLockCount++;
                    if (e.getKey() == tid) {
                        haveReadLock = true;
                    }
                } else {
                    writeLockTid = e.getKey();
                }
            }

            if (perm == Permissions.READ_ONLY) {
                // request read lock
                if (writeLockTid == null || writeLockTid == tid) {
                    acquired = true;
                }
            } else {
                // request write lock
                if (writeLockTid == tid) {
                    acquired = true;
                }

                if (writeLockTid == null) {
                    if (readLockCount == 0) {
                        acquired = true;
                    } else if (readLockCount == 1 && haveReadLock) {
                        acquired = true;
                    }
                }
            }

            if (acquired && writeLockTid == null) {
                lock.get(pid).put(tid, perm);
            }

            return acquired;
        }

        public synchronized void releaseLock(TransactionId tid) {
            for (ConcurrentHashMap<TransactionId, Permissions> m : lock.values()) {
                if (m.containsKey(tid)) {
                    m.remove(tid);
                }
            }
        }

        public synchronized void unsafeReleasePage(PageId pid, TransactionId tid) {
            lock.get(pid).remove(tid);
        }

        public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
            if (lock.containsKey(pid)) {
                if (lock.get(pid).containsKey(tid)) {
                    return true;
                }
            }

            return false;
        }

        public synchronized ArrayList<PageId> GetAssociatedPages(TransactionId tid) {
            ArrayList<PageId> pages = new ArrayList<>();

            for (PageId pid : lock.keySet()) {
                if (lock.get(pid).containsKey(tid)) {
                    pages.add(pid);
                }
            }

            return pages;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageCache = new ConcurrentHashMap<>();
        lock = new LockManager();
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
        int wait = 0;
        while (true) {
            if (lock.acquireLock(pid, tid, perm)) {
                break;
            }

            try {
                Random r = new Random();
                int time = 500 + r.nextInt(50);
                Thread.sleep(time);
                wait += time;

                if (wait > 5*1000) {
                    throw new TransactionAbortedException();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (pageCache.containsKey(pid)) {
            return pageCache.get(pid);
        }

        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = file.readPage(pid);

        evictPage();

        pageCache.put(pid, page);

        return page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lock.unsafeReleasePage(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lock.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            ArrayList<PageId> associatedPages = lock.GetAssociatedPages(tid);
            for (PageId pid : associatedPages) {
                discardPage(pid);
            }
        }

        lock.releaseLock(tid);
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
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = (ArrayList<Page>) file.insertTuple(tid, t);

        for (Page p : dirtyPages) {
            p.markDirty(true, tid);
            pageCache.put(p.getId(), p);
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
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirtyPages = (ArrayList<Page>) file.deleteTuple(tid, t);

        for (Page p : dirtyPages) {
            p.markDirty(true, tid);
            pageCache.put(p.getId(), p);
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
        for (Page p : pageCache.values()) {
            if (p.isDirty() != null) {
                flushPage(p.getId());
            }
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
        pageCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = pageCache.get(pid);

        if (p != null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(p);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        ArrayList<PageId> associatedPages = lock.GetAssociatedPages(tid);

        for (PageId pid : associatedPages) {
            try {
                flushPage(pid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        if (pageCache.size() >= numPages) {
            for (Page p : pageCache.values()) {
                if (p.isDirty() == null) {
                    discardPage(p.getId());
                }
            }
        }

        if (pageCache.size() >= numPages) {
            throw new DbException("unable evict dirty page");
        }
    }

}
