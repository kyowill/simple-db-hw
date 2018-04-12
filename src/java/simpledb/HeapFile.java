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
	private DbFileIterator iterator;
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
		iterator = null;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		// some code goes here
		// return null;
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
		// return null;
		HeapPageId id = (HeapPageId) pid;
		BufferedInputStream bis = null;
		try {
			bis = new BufferedInputStream(new FileInputStream(hf));
			byte pageBuf[] = new byte[BufferPool.getPageSize()];
			if (bis.skip((id.getPageNumber()) * BufferPool.getPageSize()) != (id
					.getPageNumber()) * BufferPool.getPageSize()) {
				throw new IllegalArgumentException(
						"Unable to seek to correct place in HeapFile");
			}
			int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
			if (retval == -1) {
				throw new IllegalArgumentException("Read past end of table");
			}
			if (retval < BufferPool.getPageSize()) {
				throw new IllegalArgumentException("Unable to read "
						+ BufferPool.getPageSize() + " bytes from HeapFile");
			}
			Debug.log(1, "HeapFile.readPage: read page %d", id.getPageNumber());
			HeapPage page = new HeapPage(id, pageBuf);
			return page;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		} finally {
			// Close the file on success or error
			try {
				if (bis != null)
					bis.close();
			} catch (IOException ioe) {
				// Ignore failures closing the file
			}
		}
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for lab1
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		// some code goes here
		// return 0;
		return (int) hf.length() / BufferPool.getPageSize();
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
			throws DbException, TransactionAbortedException {
		// some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		//return null;
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
