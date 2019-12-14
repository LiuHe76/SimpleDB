package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.DoubleAccumulator;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int maxNumPages;
    private Map<PageId, Page> pool;

    private Map<PageId, Node> cache;
    private Node sentinel;

    private class Node {
        private PageId id;
        private Node next, pre;

        public Node() {}

        public Node(PageId id) {
            this.id = id;
        }
    }

    private class Lock {
        private Permissions type;
        private PageId pid;

        public Lock(Permissions type, PageId pid) {
            this.type = type;
            this.pid = pid;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Lock)) return false;
            Lock other = (Lock) o;
            if (other.pid.equals(pid)) return true;
            return false;
        }

        @Override
        public int hashCode() {
            return pid.hashCode();
        }
    }

    Map<TransactionId, HashSet<Lock>> holds;
    Map<TransactionId, Lock> requires;

    private class LockInfo {
        private TransactionId excludeLock;
        private List<TransactionId> shareLocks = new ArrayList<>();

        public TransactionId getExcludeLock() {
            return excludeLock;
        }

        public void setExcludeLock(TransactionId excludeLock) {
            this.excludeLock = excludeLock;
        }

        public List<TransactionId> getShareLocks() {
            return shareLocks;
        }
    }

    Map<PageId, LockInfo> lockInfoMap;

    Map<PageId, TransactionId> evictInfo;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxNumPages = numPages;
        pool = new HashMap<>();

        sentinel = new Node();
        sentinel.next = sentinel;
        sentinel.pre = sentinel;
        cache = new HashMap<>();

        holds = new HashMap<>();
        requires = new HashMap<>();
        lockInfoMap = new HashMap<>();

        evictInfo = new HashMap<>();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException, IOException {

        AddToRequire(tid, pid, perm);
        checkForDeadLock(tid);
        AquireLocks(tid, pid, perm);
        addToHoldAndRemoveRequire(tid, pid, perm);

        return fetchPage(pid, tid);
    }

    private synchronized Page fetchPage(PageId pid, TransactionId tid) throws DbException, IOException {
        if (pool.containsKey(pid)) {
            moveToLast(cache.get(pid), false);
            return pool.get(pid);
        }
        if (pool.size() == maxNumPages) {
            evictPage(pid, tid);
        }
        Page page = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
        pool.put(pid, page);
        Node n = new Node(pid);
        cache.put(pid, n);
        moveToLast(n, true);

        if (evictInfo.containsKey(pid)) {
            evictInfo.remove(pid);
        }
        return page;
    }



    private synchronized void checkForDeadLock(TransactionId _tid) throws TransactionAbortedException {
        HashMap<TransactionId, Integer> tidToInt = new HashMap<>();
        int no = 0;

        HashMap<Lock, TransactionId> excludeLockInfo = new HashMap<>();
        HashMap<Lock, List<TransactionId>> shareLockInfo = new HashMap<>();

        for (TransactionId tid : holds.keySet()) {
            if (!tidToInt.containsKey(tid)) {
                tidToInt.put(tid, no++);
            }

            for (Lock lock : holds.get(tid)) {
                if (lock.type == Permissions.READ_WRITE) {
                    excludeLockInfo.put(lock, tid);
                } else {
                    if (!shareLockInfo.containsKey(lock)) {
                        shareLockInfo.put(lock, new LinkedList<>());
                    }
                    shareLockInfo.get(lock).add(tid);
                }
            }

        }


        for (TransactionId tid : requires.keySet()) {
            if (tidToInt.containsKey(tid)) continue;
            tidToInt.put(tid, no++);
        }

        DiGraph dig = new DiGraph(tidToInt.size());

        for (TransactionId tid : requires.keySet()) {
            Lock requireLock = requires.get(tid);
            if (requireLock.type == Permissions.READ_WRITE) {
                if (excludeLockInfo.containsKey(requireLock)) {
                    if (!excludeLockInfo.get(requireLock).equals(tid)) {
                        dig.addEdge(tidToInt.get(tid), tidToInt.get(excludeLockInfo.get(requireLock)));
                    }
                } else if (shareLockInfo.containsKey(requireLock)) {
                    List<TransactionId> ends = shareLockInfo.get(requireLock);
                    for (TransactionId end : ends) {
                        if (!end.equals(tid)) {
                            dig.addEdge(tidToInt.get(tid), tidToInt.get(end));
                        }
                    }
                }
            } else {
                if (excludeLockInfo.containsKey(requireLock)) {
                    if (!excludeLockInfo.get(requireLock).equals(tid)) {
                        dig.addEdge(tidToInt.get(tid), tidToInt.get(excludeLockInfo.get(requireLock)));
                    }
                }
            }
        }

        CycleDetection detector = new CycleDetection(dig);

        if (detector.hasCycle()) {
            requires.remove(_tid);
            throw new TransactionAbortedException();
        }
    }

    private class CycleDetection {
        private DiGraph dig;
        private boolean hascycle;
        private boolean[] onStack;
        private boolean[] marked;

        public CycleDetection(DiGraph dig) {
            this.dig = dig;
            onStack = new boolean[dig.V()];
            marked = new boolean[dig.V()];

            for (int i = 0; i < dig.V(); i += 1) {
                if (!marked[i]) {
                    hascycle = dfs(i);
                    if (hascycle) break;
                }
            }

        }

        public boolean hasCycle() {
            return hascycle;
        }

        private boolean dfs(int i) {
            onStack[i] = true;
            marked[i] = true;

            for (int j : dig.adj(i)) {
                if (marked[j]) {
                    if (onStack[j]) {
                        return true;
                    }
                } else {
                    if (dfs(j)) {
                        return true;
                    }
                }
            }
            onStack[i] = false;
            return false;
        }

    }

    private class DiGraph {
        private int V;
        private ArrayList<Integer>[] edges;

        public DiGraph(int V) {
            this.V = V;
            edges = (ArrayList<Integer>[]) new ArrayList[V];
            for (int i = 0; i < V; i += 1) {
                edges[i] = new ArrayList<>();
            }
        }

        public void addEdge(int i, int j) {
            edges[i].add(j);
        }

        public Iterable<Integer> adj(int i) {
            return edges[i];
        }

        public int V() {
            return V;
        }
    }





    private synchronized void AddToRequire(TransactionId tid, PageId pid, Permissions perm) {
        requires.put(tid, new Lock(perm, pid));
    }

    private synchronized void addToHoldAndRemoveRequire(TransactionId tid, PageId pid, Permissions perm) {
        requires.remove(pid);
        HashSet<Lock> tid_holds = holds.get(tid);
        if (tid_holds == null) {
            tid_holds = new HashSet<>();
            tid_holds.add(new Lock(perm, pid));
            holds.put(tid, tid_holds);
        } else {
            if (perm == Permissions.READ_WRITE) {
                tid_holds.add(new Lock(perm, pid));
            } else {
                Lock newlock = new Lock(perm, pid);
                if (!tid_holds.contains(newlock)) {
                    tid_holds.add(newlock);
                }
            }
        }
    }

    public synchronized void AquireLocks(TransactionId tid, PageId pid, Permissions perm) {
        if (holdsLock(tid, pid)) {
            if (perm == Permissions.READ_ONLY) {
                return;
            }
            LockInfo info = lockInfoMap.get(pid);
            if (info.getExcludeLock() == tid) {
                return;
            }
            if (info.getShareLocks().size() == 1) {
                releasePage(tid, pid);
                info.setExcludeLock(tid);
                return;
            }
        }
        while (true) {
            LockInfo info = lockInfoMap.get(pid);
            if (info == null) {
                info = new LockInfo();
                if (perm == Permissions.READ_ONLY) {
                    info.getShareLocks().add(tid);
                } else {
                    info.setExcludeLock(tid);
                }
                lockInfoMap.put(pid, info);
                return;
            } else {
                if (perm == Permissions.READ_ONLY) {
                    if (info.getExcludeLock() != null) {
                        try {
                            this.wait();
                        } catch (Exception e) {}
                    } else {
                        info.getShareLocks().add(tid);
                        return;
                    }
                } else {
                    if ((info.getShareLocks().contains(tid) && info.getShareLocks().size() > 1) || (!info.getShareLocks().contains(tid) && info.getShareLocks().size() > 0) || info.getExcludeLock() != null) {
                        try {
                            this.wait();
                        } catch (Exception e) {}
                    } else {
                        if (info.getShareLocks().contains(tid)) {
                            releasePage(tid, pid);
                        }
                        info.setExcludeLock(tid);
                        return;
                    }
                }
            }
        }
    }

    private void moveToLast(Node n, boolean isNew) {
        if (!isNew) {
            n.next.pre = n.pre;
            n.pre.next = n.next;
        }

        n.pre = sentinel.pre;
        sentinel.pre.next = n;
        n.next = sentinel;
        sentinel.pre = n;
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
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        LockInfo info = lockInfoMap.get(pid);
        if (info != null) {
            if (info.getExcludeLock() == tid) {
                holds.get(tid).remove(new Lock(Permissions.READ_WRITE, pid));
                info.setExcludeLock(null);
                this.notifyAll();
            } else if (info.getShareLocks().contains(tid)) {
                holds.get(tid).remove(new Lock(Permissions.READ_ONLY, pid));
                info.getShareLocks().remove(tid);
                this.notifyAll();
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        LockInfo info = lockInfoMap.get(p);
        if (info == null) return false;
        if (info.getExcludeLock() == tid || info.getShareLocks().contains(tid)) return true;
        return false;
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
        if (holds.get(tid) != null) {
            if (commit) {
                for (Lock lock : holds.get(tid)) {
                    Page page = pool.get(lock.pid);
                    if (page != null) {
                        if (page.isDirty() != null) {
                            flushPage(lock.pid);
                        }
                    }
                }
            } else {
                for (Lock lock : holds.get(tid)) {
                    Page page = pool.get(lock.pid);
                    if (page != null) {
                        if (page.isDirty() != null) {
                            pool.put(lock.pid, Database.getCatalog().getDbFile(lock.pid.getTableId()).readPage(lock.pid));
                        }
                    }
                }
            }

            releaseLocks(tid);
        }
    }

    private synchronized void releaseLocks(TransactionId tid) {
        for (Lock lock : holds.get(tid)) {
            if (lock.type == Permissions.READ_ONLY) {
                lockInfoMap.get(lock.pid).getShareLocks().remove(tid);
            } else {
                lockInfoMap.get(lock.pid).setExcludeLock(null);
            }
        }
        holds.remove(tid);
        this.notifyAll();
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDbFile(tableId);
        List<Page> pages = heapFile.addTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException, IOException {
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
        Page page = heapFile.deleteTuple(tid, t);
        page.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId id : pool.keySet()) {
            flushPage(id);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = pool.get(pid);
        if (page.isDirty() != null) {
            Database.getCatalog().getDbFile(page.getId().getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */

    private synchronized void evictPage(PageId _pid, TransactionId _tid) throws DbException {
        Node to_delete = sentinel.next;

        while (true) {
            if (to_delete == sentinel) {
                releasePage(_tid, _pid);
                throw new DbException("evict error.");
            }
            if (pool.get(to_delete.id).isDirty() == null && NoLockAllocated(to_delete.id)) {
                break;
            } else if (pool.get(to_delete.id).isDirty() != null) {
                to_delete = to_delete.next;
            } else {
                LockInfo lockInfo = lockInfoMap.get(to_delete.id);
                if (lockInfo.getExcludeLock() == _tid || (lockInfo.getShareLocks().size() == 1 && lockInfo.getShareLocks().contains(_tid))) {
                    evictInfo.put(to_delete.id, _tid);
                    break;
                }
                to_delete = to_delete.next;
            }
        }

        to_delete.next.pre = to_delete.pre;
        to_delete.pre.next = to_delete.next;

        PageId pid = to_delete.id;
        cache.remove(pid);
        pool.remove(pid);
    }

    private boolean NoLockAllocated(PageId pid) {
        LockInfo lockInfo = lockInfoMap.get(pid);
        if (lockInfo == null) return true;
        if (lockInfo.getExcludeLock() == null && lockInfo.getShareLocks().size() == 0) return true;
        return false;
    }

}
