package simpledb;

import java.io.*;
import java.util.*;



/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	private final File hf;
	private final TupleDesc td;
	private final int tableid;
	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td) {
		// some code goes here
		this.hf = f;
		this.td = td;
		this.tableid = f.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		// some code goes here
		return hf;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere to ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		// some code goes here
		// throw new UnsupportedOperationException("implement this");
		return tableid;
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 *
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		// throw new UnsupportedOperationException("implement this");
		return td;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		// some code goes here
		HeapPageId id = (HeapPageId) pid;
		RandomAccessFile raf = null;
		HeapPage page = null;
		try {
			raf = new RandomAccessFile(hf, "r");
			byte pageBuf[] = new byte[BufferPool.getPageSize()];
			raf.seek(1L*id.getPageNumber() * BufferPool.getPageSize());
			raf.read(pageBuf, 0, BufferPool.getPageSize());
			 page = new HeapPage(id, pageBuf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}finally{
			try {
				raf.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return page;
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for lab1
		byte[] data = page.getPageData();
		RandomAccessFile rf = new RandomAccessFile(hf, "rw");
		rf.seek(1L * page.getId().getPageNumber() * BufferPool.getPageSize());
		rf.write(data,  0, BufferPool.getPageSize());
		rf.close();
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		// some code goes here
		return (int) (hf.length() / BufferPool.getPageSize());
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		ArrayList<Page> arrayList = new ArrayList<Page>();
		int i = 0;
		for(; i < numPages(); ++ i){
			HeapPageId pid = new HeapPageId(getId(), i);
			HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			if(page.getNumEmptySlots() > 0){
				page.insertTuple(t);
				arrayList.add(page);
				break;
			}
		}
		if(i == numPages()){
    		HeapPage page = new HeapPage(new HeapPageId(getId(), i), HeapPage.createEmptyPageData());
    		page.insertTuple(t);
    		arrayList.add(page);
    		writePage(page);
		}
		return arrayList;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
			throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
    	BufferPool pool = Database.getBufferPool();
    	HeapPage page = (HeapPage)pool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	page.deleteTuple(t);
		ArrayList<Page> arrayList = new ArrayList<Page>();
		arrayList.add(page);
		return arrayList;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		return new HeapFileIterator(tid, this);
	}

	class HeapFileIterator extends AbstractDbFileIterator {

		TransactionId tid = null;
		HeapFile file = null;
		Iterator<Tuple> it = null;
		int cursor = 0;

		public HeapFileIterator(TransactionId tid, HeapFile file) {
			this.tid = tid;
			this.file = file;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			HeapPageId pid = new HeapPageId(file.getId(), cursor);// starts with
																	// zero
			HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,
					pid, Permissions.READ_ONLY);
			it = page.iterator();
			//cursor = 0;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			close();
			open();
		}

		@Override
		protected Tuple readNext() throws DbException,
				TransactionAbortedException {
			// TODO Auto-generated method stub
			// return null;
			while (it != null) {
				while (it.hasNext()) {
					return it.next();
				}
				if (cursor < (file.numPages() - 1)) {
					HeapPageId pid = new HeapPageId(file.getId(), cursor + 1);
					HeapPage page = (HeapPage) Database.getBufferPool()
							.getPage(tid, pid, Permissions.READ_ONLY);
					it = page.iterator();
					cursor++;
				} else {
					return null;
				}
			}
			return null;
		}

		public void close() {
			super.close();
			it = null;
			cursor = 0;
		}
		
	}
}
