package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int [] b;
    private int width;
    private int total;

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
        b = new int[buckets];

        width = (max - min) / buckets;
        if (width == 0) {
            width = 1;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        b[getBucketIndex(v)]++;
        total++;
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
        if (op == Predicate.Op.EQUALS) {
            if (v < min || v > max) {
                return 0.0;
            }
            return b[getBucketIndex(v)] * 1.0 / width / total;
        } else if (op == Predicate.Op.LESS_THAN) {
            if (v < min) {
                return 0.0;
            } else if (v > max) {
                return 1.0;
            }

            int index = getBucketIndex(v);

            double r = 0.0;

            for (int i=0; i<index; i++) {
                r += b[i] * 1.0 / total;
            }

            return r;
        } else if (op == Predicate.Op.GREATER_THAN) {
            if (v < min) {
                return 1.0;
            } else if (v > max) {
                return 0.0;
            }

            int index = getBucketIndex(v);

            double r = 0.0;

            for (int i=index+1; i<buckets; i++) {
                r += b[i] * 1.0 / total;
            }

            return r;
        } else if (op == Predicate.Op.NOT_EQUALS) {
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return -1.0;
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
        return null;
    }

    public int getBucketIndex(int v) {
        int id = (v-min) / width;
        if (id >= buckets) {
            id = buckets - 1;
        }
        return id;
    }
}
