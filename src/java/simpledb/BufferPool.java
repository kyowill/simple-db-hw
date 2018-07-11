package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {

	/** Bytes per page, including header. */
	private static final int DEFAULT_PAGE_SIZE = 4096;

	private static int pageSize = DEFAULT_PAGE_SIZE;

	private int numPages;

	private LockManager lockManager;
	private Map<PageId, Page> buffers;
	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// some code goes here
		this.numPages = numPages;
		this.lockManager = new LockManager();
		this.buffers = new ConcurrentHashMap<PageId, Page>();
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
	 * @throws DbException 
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, a page should be evicted and the new page should be added in
	 * its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 * @throws  
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// some code goes here
		Page p = buffers.get(pid);
		try {
			lockManager.acquireLock(tid, perm, pid);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(p == null){
			p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
			if(buffers.size() > numPages){
				evictPage();
			}
			buffers.put(pid, p);
		}
		return p;
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 * @throws InterruptedException 
	 */
	public void releasePage(TransactionId tid, PageId pid) throws InterruptedException {
		// some code goes here
		// not necessary for lab1|lab2
		lockManager.releaseLock(tid, pid);
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @throws InterruptedException 
	 */
	public void transactionComplete(TransactionId tid) throws IOException{
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		return lockManager.isHoldLock(tid, pid);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 * @throws InterruptedException 
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException{
		// some code goes here
		// not necessary for lab1|lab2
		if(commit){
			try{
				flushPages(tid);
			}catch(IOException e){
				e.printStackTrace();
			}
		}else{
    		for (PageId pid: buffers.keySet())
    		{
    			Page p = buffers.get(pid);
    			if (tid.equals(p.isDirty()))
    			{
    				buffers.put(pid, p.getBeforeImage());
    				p.markDirty(false, null);
    			}
    		}
		}
		for (PageId pid: buffers.keySet())
		{
			if (lockManager.isHoldLock(tid, pid)){
				try {
					lockManager.releaseLock(tid, pid);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to and any other
	 * pages that are updated (Lock acquisition is not needed for lab2). May
	 * block if the lock(s) cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have been
	 * dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		ArrayList<Page> arrayList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
		Iterator<Page> it = arrayList.iterator();
		while(it.hasNext()){
			Page nextPage = it.next();
			nextPage.markDirty(true, tid);
			buffers.put(nextPage.getId(), nextPage);
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is removed from and any other pages that are
	 * updated. May block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have been
	 * dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction deleting the tuple.
	 * @param t
	 *            the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException,
			IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		RecordId rid = t.getRecordId();
		PageId pid = rid.getPageId();
		int tableId = pid.getTableId();
		ArrayList<Page> arrayList = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
		Iterator<Page> it = arrayList.iterator();
		while(it.hasNext()){
			Page nextPage = it.next();
			nextPage.markDirty(true, tid);
			buffers.put(nextPage.getId(), nextPage);
		}
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for lab1
    	for (PageId pid:buffers.keySet())
    	{
    		flushPage(pid);
    	}
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 * 
	 * Also used by B+ tree files to ensure that deleted pages are removed from
	 * the cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for lab1
		buffers.remove(pid);
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
		// not necessary for lab1
		Page p = buffers.get(pid);
		if(p != null){
			Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
			p.markDirty(false, null);
		}
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
    	for (PageId pid:buffers.keySet())
    	{
    		if(buffers.get(pid) != null && holdsLock(tid, pid)){
    			flushPage(pid);
    		}
    	}
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 * @throws  
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		// not necessary for lab1
		for (PageId pid:buffers.keySet()){
			try {
				if(buffers.get(pid).isDirty() == null){
					flushPage(pid);
					buffers.remove(pid);
					return;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
