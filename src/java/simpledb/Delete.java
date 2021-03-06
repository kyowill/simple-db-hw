package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    //private int times;
    private TupleDesc td;
    //private ArrayList<Tuple> recs = new ArrayList<Tuple>();
    //private Iterator<Tuple> iter = null;
    private boolean fetched = false;
    //private boolean deleted = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	tid = t;
    	this.child = child;
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
    	child.open();
    	//deleteTuples();
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
    	//deleteTuples();
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
       			//System.out.println("delete loop thread id:"+Thread.currentThread().getId() + ":" + ((IntField)tup.getField(0)).getValue());
       			Database.getBufferPool().deleteTuple(tid, tup);
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
}
