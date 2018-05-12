package simpledb;

import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int buckets;
	private int min;
	private int max;
	private int interval;
	private HashMap<Integer, Integer> histogram;
	private int ntups;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	this.interval = (int) Math.ceil((double)(max - min + 1) / buckets);
    	this.ntups = 0;
    	histogram = new HashMap<Integer, Integer>();
    	int key = 0;
    	while(key < buckets){
    		histogram.put(key, 0);
    		key += 1;
    	}
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	
    	int key = (int) Math.ceil((double)(v - min + 1) / interval) - 1;
/*    	HashMap<Integer, Integer> map = histogram.get(key);
    	if(map == null){
    		System.out.println("add value error!");
    		return;
    	}
    	
    	Integer val = map.get(v);
    	if(val != null){
    		map.put(v, val.intValue() + 1);
    		//System.out.println("+1");
    	}else{
    		map.put(v, 1);
    	}*/
    	Integer val = histogram.get(key);
    	histogram.put(key, val.intValue() + 1);
    	ntups += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        //return -1.0;
/*    	if(v < min){
    		v = min;
    	}
    	if(v > max){
    		v = max;
    	}*/
    	int key = (int) Math.ceil((double)(v - min + 1) / interval) - 1;
    	
    	if(op.toString().equals("=")){
    		if((v < min) || (v > max)){
    			return 0;
    		}
    		double h = (double) histogram.get(key).intValue();
        	double w = (double) interval;
        	return (h / w) / ntups;
    	}else if(op.toString().equals(">")){
    		if(v >= max){
    			return 0;
    		}
    		if(v < min ){
    			return 1.0;
    		}
    		int right = min + (key + 1) * interval;
    		int part = (right - v - 1) / interval;
    		int h = histogram.get(key).intValue();
    		int w = interval;
    		int big = key + 1;
    		double num = (double) h * part / interval;
    		while((big) < histogram.size()){
    			num += histogram.get(big);
    			big += 1;
    		}
    		return (double) num / ntups;
    	}else if (op.toString().equals("<")){
    		if(v > max){
    			return 1.0;
    		}
    		if(v <= min ){
    			return 0.0;
    		}
    		int left = min + key * interval;
    		int part = (v - left - 1) / interval;
    		int h = histogram.get(key).intValue();
    		int w = interval;
    		int small = key - 1;
    		double num = (double) h * part / interval;
    		while((small) >= 0){
    			num += histogram.get(small);
    			small -= 1;
    		}
    		return (double) num / ntups;
    	}else if (op.toString().equals(">=")){
    		v -= 1;
    		if(v >= max){
    			return 0;
    		}
    		if(v < min ){
    			return 1.0;
    		}
    		key = (int) Math.ceil((double)(v - min + 1) / interval) - 1;
    		int right = min + (key + 1) * interval;
    		int part = (right - v - 1) / interval;
    		int h = histogram.get(key).intValue();
    		int w = interval;
    		int big = key + 1;
    		double num = (double) h * part / interval;
    		while((big) < histogram.size()){
    			num += histogram.get(big);
    			big += 1;
    		}
    		return (double) num / ntups;
    	}else if (op.toString().equals("<=")){
    		v += 1;
    		if(v > max){
    			return 1.0;
    		}
    		if(v <= min ){
    			return 0.0;
    		}
        	key = (int) Math.ceil((double)(v - min + 1) / interval) - 1;
    		int left = min + key * interval;
    		int part = (v - left - 1) / interval;
    		int h = histogram.get(key).intValue();;
    		int w = interval;
    		int small = key - 1;
    		double num = (double) h * part / interval;
    		while((small) >= 0){
    			num += histogram.get(small);
    			small -= 1;
    		}
    		return (double) num / ntups;
    	}else if(op.toString().equals("<>")){
    		double h = (double) histogram.get(key).intValue();
        	double w = (double) interval;
        	return 1 - (h / w) / ntups;
    	}else {
    		return -1.0;
    	}
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        //return null;
    	return buckets + ":" + min + ":" + max + ":" + interval;
    }
}
