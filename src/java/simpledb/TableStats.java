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
    private int numPages=0;
    private int cardinality = 0;
    private HashMap<Integer, IntHistogram> intHistograms = new HashMap<Integer, IntHistogram>();
    private HashMap<Integer, StringHistogram> strHistograms = new HashMap<Integer, StringHistogram>();
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
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	DbFileIterator iter = file.iterator(new TransactionId());
    	TupleDesc td=file.getTupleDesc();
    	HashMap<Integer, Integer> mins = new HashMap<Integer, Integer>();
    	HashMap<Integer, Integer> maxs = new HashMap<Integer, Integer>();
    	for(int i=0; i < td.numFields(); ++i){
    		if(td.getFieldType(i).equals(Type.INT_TYPE)){
    			mins.put(i, Integer.MIN_VALUE);
    			maxs.put(i, Integer.MAX_VALUE);
    		}else{
    			strHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
    		}
    	}
    	try {
    		iter.open();
			while(iter.hasNext()){
				Tuple t = iter.next();
				cardinality ++;
				for(int i=0; i < td.numFields(); ++i){
					if(td.getFieldType(i).equals(Type.INT_TYPE)){
						IntField field=(IntField)t.getField(i);
						if(mins.get(i).equals(Integer.MIN_VALUE)){
							mins.put(i, field.getValue());
						}
						if(maxs.get(i).equals(Integer.MAX_VALUE)){
							maxs.put(i, field.getValue());
						}
						if(field.getValue() < mins.get(i)){
							mins.put(i, field.getValue());
						}
						if(field.getValue() > maxs.get(i)){
							maxs.put(i, field.getValue());
						}
					}
				}
			}
	    	for(int i=0; i < td.numFields(); ++i){
	    		if(td.getFieldType(i).equals(Type.INT_TYPE)){
	    			intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, mins.get(i), maxs.get(i)));
	    		}
	    	}
			iter.rewind();
			while(iter.hasNext()){
				Tuple t = iter.next();
				for(int i=0; i < td.numFields(); ++i){
					if(td.getFieldType(i).equals(Type.INT_TYPE)){
						IntField field=(IntField)t.getField(i);
						intHistograms.get(i).addValue(field.getValue());
					}else{
						StringField field = (StringField)t.getField(i);
						strHistograms.get(i).addValue(field.getValue());
					}
				}
			}
			int pageSize=BufferPool.getPageSize();
			//numPages = (cardinality * td.getSize() + pageSize - 1)/ pageSize;
			numPages = (int) Math.ceil((double)cardinality * td.getSize() / pageSize);
			//double tuplesNum = Math.floor((pageSize * 8) / (td.getSize() * 8 + 1));
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			iter.close();
		}
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
    	return ioCostPerPage * numPages;
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
    	return (int) (cardinality * selectivityFactor);
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
        if (strHistograms.containsKey(field)) {
            return strHistograms.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        } else {
            return intHistograms.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return cardinality;
    }
}
