package simpledb;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import simpledb.Predicate.Op;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator child;
    private int tableId;
    //private int times;
    private TupleDesc td;
    //private ArrayList<Tuple> recs = new ArrayList<Tuple>();
    //private Iterator<Tuple> iter = null;
    private boolean fetched = false;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	transactionId = t;
    	this.child = child;
    	this.tableId = tableId;
    	TupleDesc tDesc = Database.getCatalog().getDatabaseFile(tableId).getTupleDesc();
    	//TupleDesc tDesc1 = child.getTupleDesc();
    	if(!child.getTupleDesc().equals(tDesc)){
    		throw new DbException("TupleDesc of child differs from table!");
    	}
    	Type[] typeAr = new Type[]{Type.INT_TYPE};
    	td = new TupleDesc(typeAr);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //return null;
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	//insertTuples();
    	//iter = recs.iterator();
    	child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	//iter = recs.iterator();
    	close();
    	open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @throws IOException 
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException{
        // some code goes here
        //return null;
/*    	if(iter.hasNext()){
    		return iter.next();
    	}
    	return null;*/
       	Tuple t = new Tuple(td);
       	int count=0;
       	try{
       		if (fetched)
       			return null;
       		fetched = true;
       		while(child.hasNext())
       		{
       			Tuple tup = child.next();
       			System.out.println("insert loop-"+Thread.currentThread().getId() + ":" + ((IntField)tup.getField(0)).getValue());
       			Database.getBufferPool().insertTuple(transactionId, tableId, tup);
       			count++;
       		}
       	}
       	catch (DbException e)
       	{
       		e.printStackTrace();
       	}
       	catch (IOException e)
       	{
       		e.printStackTrace();
       	}
       	Field fd = new IntField(count);
       	t.setField(0, fd);
       	return t;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        //return null;
    	return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	child = children[0];
    }
    
/*    private void insertTuples() throws DbException, TransactionAbortedException{
    	child.open();
    	while(child.hasNext()){
    		Tuple t = child.next();
    		try {
				Database.getBufferPool().insertTuple(transactionId, tableId, t);
				times += 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				break;
			}
    	}
    	child.close();
		Tuple rec = new Tuple(td);
		rec.setField(0, new IntField(times));
		recs.clear();
		recs.add(rec);
    }*/
}
