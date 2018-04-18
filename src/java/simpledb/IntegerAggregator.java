package simpledb;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, List<Integer>> groups;
    private Map<Field, Tuple> results;
    private TupleDesc td;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groups = new HashMap<Field, List<Integer>>();
        results = new HashMap<Field, Tuple>();
        Type[] types;
        if(gbfield == Aggregator.NO_GROUPING){
            types = new Type[]{Type.INT_TYPE};
        }else{
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
        }
        td = new TupleDesc(types);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = tup.getField(gbfield);
        Integer val = tup.getField(afield).hashCode();
        if(groups.get(key) == null){
            groups.put(key, new LinkedList<Integer>());
        }
        groups.get(key).add(val);
        Iterator<Integer> iter = groups.get(key).iterator();
        Integer aggregateVal = doAggregate(what.toString(), iter);
        Tuple t = new Tuple(td);
        if(gbfield == Aggregator.NO_GROUPING){
            Field f1 = new IntField(aggregateVal);
            t.setField(0, f1);
        }else{
            Field f1 = tup.getField(gbfield);
            Field f2 = new IntField(aggregateVal);
            t.setField(0, f1);
            t.setField(1, f2);
        }
        results.put(key, t);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new
        //UnsupportedOperationException("please implement me for lab2");
        return new TupleIterator(td, results.values());
    }

    private Integer doAggregate(String type, Iterator<Integer> it){
        if(type.equals("min")){
            Integer min = null;
            while (it.hasNext()){
                Integer next = it.next();
                if(min == null){
                    min = next;
                    continue;
                }
                int a = min.intValue();
                int b = next.intValue();
                if(a > b){
                    min = next;
                }
            }
            return min;
        }else if(type.equals("max")){
            Integer max = null;
            while (it.hasNext()){
                Integer next = it.next();
                if(max == null){
                    max = next;
                    continue;
                }
                int a = max.intValue();
                int b = next.intValue();
                if(a < b){
                    max = next;
                }
            }
            return max;
        }else if(type.equals("sum")){
            Integer sum = 0;
            while (it.hasNext()){
                Integer next = it.next();
                sum += next;
            }
            return sum;
        }else if(type.equals("count")){
            Integer cnt = 0;
            while (it.hasNext()){
                Integer next = it.next();
                cnt += 1;
            }
            return cnt;
        }else if(type.equals("avg")){
            Integer sum = 0;
            Integer cnt = 0;
            while (it.hasNext()){
                Integer next = it.next();
                sum += next;
                cnt += 1;
            }
            Integer avg = sum / cnt;
            return avg;
        }
        throw new UnsupportedOperationException("operator illegal!");
    }
}
