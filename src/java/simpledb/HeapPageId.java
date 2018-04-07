package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

	private int tid;
	private int pn;

	/**
	 * Constructor. Create a page id structure for a specific page of a specific
	 * table.
	 *
	 * @param tableId
	 *            The table that is being referenced
	 * @param pgNo
	 *            The page number in that table.
	 */
	public HeapPageId(int tableId, int pgNo) {
		// some code goes here
		this.tid = tableId;
		this.pn = pgNo;
	}

	/** @return the table associated with this PageId */
	public int getTableId() {
		// some code goes here
		// return 0;
		return this.tid;
	}

	/**
	 * @return the page number in the table getTableId() associated with this
	 *         PageId
	 */
	public int getPageNumber() {
		// some code goes here
		// return 0;
		return this.pn;
	}

	/**
	 * @return a hash code for this page, represented by the concatenation of
	 *         the table number and the page number (needed if a PageId is used
	 *         as a key in a hash table in the BufferPool, for example.)
	 * @see BufferPool
	 */
	public int hashCode() {
		// some code goes here
		// throw new UnsupportedOperationException("implement this");
		int t = this.tid;
		int p = this.pn;
		String s = t + ":" + p;
		return s.hashCode();
	}

	/**
	 * Compares one PageId to another.
	 *
	 * @param o
	 *            The object to compare against (must be a PageId)
	 * @return true if the objects are equal (e.g., page numbers and table ids
	 *         are the same)
	 */
	public boolean equals(Object o) {
		// some code goes here
		// return false;
		PageId pId = null;
		try {
			pId = (PageId) o;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		if ((pId.getPageNumber() == this.pn) && (pId.getTableId() == this.tid)) {
			return true;
		}

		return false;
	}

	/**
	 * Return a representation of this object as an array of integers, for
	 * writing to disk. Size of returned array must contain number of integers
	 * that corresponds to number of args to one of the constructors.
	 */
	public int[] serialize() {
		int data[] = new int[2];

		data[0] = getTableId();
		data[1] = getPageNumber();

		return data;
	}

}
