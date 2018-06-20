package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

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

/*	public static class Buffer {
		private Page page;
		private TransactionId tid;
		private Permissions perm;

		public Buffer(TransactionId t, Page p, Permissions perm) {
			this.page = p;
			this.tid = t;
			this.perm = perm;
		}

		public Page getPage() {
			return this.page;
		}
		
		public PageId getPageId(){
			return this.page.getId();
		}

		public TransactionId getTransactionId() {
			return tid;
		}

		public void accessBuffer(TransactionId t, Permissions perm) {
			this.tid = t;
			this.perm = perm;
		}

	}*/

	/** Bytes per page, including header. */
	private static final int DEFAULT_PAGE_SIZE = 4096;

	private static int pageSize = DEFAULT_PAGE_SIZE;

	private int numPages;

	private Page[] buffers;
	private LockManager lockManager;
	private int validNum;
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
		this.buffers = new Page[numPages];
		this.lockManager = new LockManager(numPages);
		this.validNum = 0;
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
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// some code goes here
		
/*		Buffer buf = null;
		if (this.buffers.get(pid) == null) {
			if (this.buffers.size() == numPages) {
				//throw new DbException("buffer pool is full!");//lab 1.x
				evictPage();//lab 2.5
			}
			Page page = Database.getCatalog().getDatabaseFile(pid.getTableId())
					.readPage(pid);
			buf = new Buffer(tid, page, perm);
			this.buffers.put(pid, buf);
			return page;
		}
		buf = this.buffers.get(pid);
		if (!buf.getTransactionId().equals(tid)) {
			buf.accessBuffer(tid, perm);
		}
		return buf.getPage();*/
		Page pg = null;
		int pos = -1;
		int none = 0;
		for(int i = 0; i < numPages; ++i){
			if(buffers[i] != null && buffers[i].getId().equals(pid)){
				pos = i;
			}else if (buffers[i] == null){
				none = i;
			}
		}
		
		if(pos == -1){
			if(validNum == numPages){
				evictPage();// lab 2.5
			}
			Page page = Database.getCatalog().getDatabaseFile(pid.getTableId())
					.readPage(pid);
			buffers[none] = page;
			validNum += 1;
			pg = page;
		}else{
			pg = buffers[pos];
		}
		return pg;
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
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
		int pos = -1;
		for(int i = 0; i < numPages; ++i){
			if(buffers[i] != null && buffers[i].getId().equals(p)){
				pos = -1;
				break;
			}
		}
		return lockManager.isHoldLock(tid, pos);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
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
/*		ArrayList<Page> arrayList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
		Iterator<Page> it = arrayList.iterator();
		while(it.hasNext()){
			Page nextPage = it.next();
			Buffer buffer = buffers.get(nextPage.getId());
			if(buffer == null){
				buffer = new Buffer(tid, nextPage, Permissions.READ_WRITE);
				buffers.put(nextPage.getId(), buffer);
			}
			buffer.accessBuffer(tid, Permissions.READ_WRITE);
			nextPage.markDirty(true, tid);
		}*/	
		RecordId rid = t.getRecordId();
		PageId pid = rid.getPageId();
		Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
		for(int i = 0; i < numPages; ++i){
			if(buffers[i] != null && buffers[i].getId().equals(pid)){
				buffers[i].markDirty(true, tid);
				break;
			}
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
/*		ArrayList<Page> arrayList = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
		Iterator<Page> it = arrayList.iterator();
		while(it.hasNext()){
			Page nextPage = it.next();
			//Buffer buffer = buffers.get(nextPage.getId());
			
			if(buffer == null){
				buffer = new Buffer(tid, nextPage, Permissions.READ_WRITE);
				buffers.put(nextPage.getId(), buffer);
			}
			buffer.accessBuffer(tid, Permissions.READ_WRITE);
			nextPage.markDirty(true, tid);
		}*/	
		Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
		for(int i = 0; i < numPages; ++i){
			if(buffers[i] != null && buffers[i].getId().equals(pid)){
				buffers[i].markDirty(true, tid);
				break;
			}
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
/*		for (Map.Entry<PageId, Buffer> entry : buffers.entrySet()) {
			pid = entry.getKey();
			flushPage(entry.getKey());		
		}*/
		for(int i = 0; i < numPages; ++i){
			if(buffers[i] != null){
				flushPage(buffers[i].getId());
			}
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
		//buffers.remove(pid);
		for(int i=0; i < numPages; ++i){
			if(buffers[i] != null && buffers[i].getId().equals(pid)){
				buffers[i] = null;
				validNum -= 1;
				break;
			}
		}
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
		//Buffer buf = buffers.get(pid);
		Page buf = null;
		for(int i = 0; i < numPages; ++i){
			if(buffers[i] != null && buffers[i].getId().equals(pid)){
				buf = buffers[i];
				break;
			}
		}
		if(buf == null){
			throw new IOException("page not exist in bufferpool");
		}
		//Page pg = buf.getPage();
		Page pg = buf;
		Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pg);
		//mark not dirty
		//pg.markDirty(false, buf.getTransactionId());
		pg.markDirty(false, buf.isDirty());
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 * @throws  
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		// not necessary for lab1
		PageId pid = null; 
/*		for (Map.Entry<PageId, Buffer> entry : buffers.entrySet()) {  
			pid = entry.getKey();
			break;
		}*/
		int idx = 0;
		for(int i=0; i < numPages; i++){
			if(buffers[i] != null){
				pid = buffers[i].getId();
				idx = i;
				break;
			}
		}
		
		try {
			flushPage(pid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new DbException("can not flush page" + pid.toString());
		}
		//buffers.remove(pid);
		buffers[idx] = null;
		validNum -= 1;
	}

}
