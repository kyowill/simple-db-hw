package simpledb;

import java.util.HashMap;


/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int min;
	private int max;
	private double width;
	private int[] histogram;
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
    	this.histogram = new int[Math.min(buckets, max-min+1)];
    	this.min = min;
    	this.max = max + 1;
    	this.width = (1.0 + max- min)/this.histogram.length;;
    	this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v >= min && v < max) {
            histogram[getIndex(v)]++;
            ntups++;
        }
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
        if (op.equals(Predicate.Op.LESS_THAN)) {
            if (v <= min) {
                return 0;
            } else if (v >= max) {
                return 1;
            } else {
                final int idx = getIndex(v);
                double cnt = 0;
                for (int i=0; i<idx; i++) {
                    cnt += histogram[i];
                }
                cnt += histogram[idx]/width*(v-idx*width-min);
                return cnt/ntups;
            }
        }
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v+1);
        }
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            return 1-estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        }
        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v-1);
        }
        if (op.equals(Predicate.Op.EQUALS)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) -
                    estimateSelectivity(Predicate.Op.LESS_THAN, v);
        }
        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0.0;
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
    	return histogram.length + ":" + min + ":" + max + ":" + width;
    }
    
    private int getIndex(int v) {
        if (v < min || v >= max) {
            throw new IllegalArgumentException(
                    String.format("value %d out of [%d, %d)", v, min, max));
        }
        return (int)((v-min)/width);
    }
    
}
