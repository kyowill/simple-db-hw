package simpledb;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.corba.se.spi.ior.MakeImmutable;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid;
    private int ioCostPerPage;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	this.tableid = tableid;
    	this.ioCostPerPage = ioCostPerPage;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        //return 0;
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	HeapFile hfFile = (HeapFile) file;
    	return ioCostPerPage * hfFile.numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        //return 0;
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	HeapFile hfFile = (HeapFile) file;
    	int pageNum = hfFile.numPages();
    	return (int) (pageNum * totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        //return 1.0;
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	HeapFile hfFile = (HeapFile) file;
    	TupleDesc td = file.getTupleDesc();
    	Type tp = td.getFieldType(field);
    	try{
    		if(tp == Type.INT_TYPE){
    			IntHistogram ih = getIntHistogram(field);
    			int v = ((IntField)constant).getValue();
    			return ih.estimateSelectivity(op, v);
    		}else if(tp == Type.STRING_TYPE){
    			StringHistogram sh = getStringHistogram(field);
    			String s = ((StringField)constant).getValue();
    			return sh.estimateSelectivity(op, s);
    		}else{
    			return 1.0;
    		}
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    	return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	HeapFile hfFile = (HeapFile) file;
        int tupleSize = hfFile.getTupleDesc().getSize();
        int pageSize = BufferPool.getPageSize();
        double tuplesNum = Math.floor((pageSize * 8) / (tupleSize * 8 + 1));
        return (int) tuplesNum;
    }

    private IntHistogram getIntHistogram(int field) throws FileNotFoundException{
    	
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	HeapFile hfFile = (HeapFile) file;
    	BufferedInputStream bis;
		bis = new BufferedInputStream(new FileInputStream(hfFile.getFile()));
    	IntHistogram ih = new IntHistogram(NUM_HIST_BINS, 0, 100);
    	int pgNo = 0;
    	try{
	    	while(true){
	    		HeapPageId id = new HeapPageId(tableid, pgNo);
	    		byte pageBuf[] = new byte[BufferPool.getPageSize()];
				if (bis.skip((id.getPageNumber()) * BufferPool.getPageSize()) != (id
						.getPageNumber()) * BufferPool.getPageSize()) {
					//throw new IllegalArgumentException(
					//		"Unable to seek to correct place in HeapFile");
					break;	
				}
				int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
	    		if(retval == -1){
	    			break;
	    		}
	    		HeapPage page = new HeapPage(id, pageBuf);
	    		Iterator<Tuple> it = page.iterator();
	    		while (it.hasNext()){
	    			IntField f = (IntField)it.next().getField(field);
	    			ih.addValue(f.getValue());
	    		}
	    		pgNo += 1;
	    	}
    	}catch(Exception e){
    		e.printStackTrace();
    	}finally {
			// Close the file on success or error
			try {
				if (bis != null)
					bis.close();
			} catch (IOException ioe) {
				// Ignore failures closing the file
			}
			
		}
    	return ih;
    }
    
    private StringHistogram getStringHistogram(int field) throws FileNotFoundException{
    	
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	HeapFile hfFile = (HeapFile) file;
    	BufferedInputStream bis;
		bis = new BufferedInputStream(new FileInputStream(hfFile.getFile()));
    	StringHistogram ih = new StringHistogram(NUM_HIST_BINS);
    	int pgNo = 0;
    	try{
	    	while(true){
	    		HeapPageId id = new HeapPageId(tableid, pgNo);
	    		byte pageBuf[] = new byte[BufferPool.getPageSize()];
				if (bis.skip((id.getPageNumber()) * BufferPool.getPageSize()) != (id
						.getPageNumber()) * BufferPool.getPageSize()) {
					throw new IllegalArgumentException(
							"Unable to seek to correct place in HeapFile");
				}
				int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
	    		if(retval == -1){
	    			break;
	    		}
	    		HeapPage page = new HeapPage(id, pageBuf);
	    		Iterator<Tuple> it = page.iterator();
	    		while (it.hasNext()){
	    			StringField f = (StringField)it.next().getField(field);
	    			ih.addValue(f.getValue());
	    		}
	    		pgNo += 1;
	    	}
    	}catch(Exception e){
    		e.printStackTrace();
    	}finally {
			// Close the file on success or error
			try {
				if (bis != null)
					bis.close();
			} catch (IOException ioe) {
				// Ignore failures closing the file
			}
			
		}
    	return ih;
    }
}
