package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private HashMap<Field, ArrayList<Integer>> group;

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
        this.op = what;

        group = new HashMap<>();
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
        Field k = null;
        IntField v = (IntField) tup.getField(afield);

        if (gbfield != NO_GROUPING) {
            k = tup.getField(gbfield);
        }

        if (!group.containsKey(k)) {
            group.put(k, new ArrayList<>());
        }

        group.get(k).add(v.getValue());
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
        return new IntegerAggIterator(group, gbfieldtype, op);
    }

    private class IntegerAggIterator implements OpIterator {
        private HashMap<Field, ArrayList<Integer>> group;
        private Type gbtype;
        private Op op;
        private Iterator iter;

        public IntegerAggIterator(HashMap<Field, ArrayList<Integer>> group, Type gbtype, Op op) {
            this.group = group;
            this.gbtype = gbtype;
            this.op = op;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = group.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Map.Entry<Field, ArrayList<Integer>> e = (Map.Entry<Field, ArrayList<Integer>>) iter.next();

            int r = compute(e.getValue(), op);

            TupleDesc td = getTupleDesc();
            Tuple t = null;

            if (gbfield != NO_GROUPING) {
                t = new Tuple(td);
                t.setField(0, e.getKey());
                t.setField(1, new IntField(r));
            } else {
                t = new Tuple(td);
                t.setField(0, new IntField(r));
            }

            return t;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            Type typeAr[] = null;

            if (gbfield != NO_GROUPING) {
                typeAr = new Type[2];
                typeAr[0] = gbtype;
                typeAr[1] = Type.INT_TYPE;
            } else {
                typeAr = new Type[1];
                typeAr[0] = Type.INT_TYPE;
            }

            return new TupleDesc(typeAr);
        }

        @Override
        public void close() {
            iter = null;
        }

        private int compute(ArrayList<Integer> l, Op op) {
            int sum = 0;
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;

            if (op == Op.COUNT) {
                return l.size();
            }

            for (int i=0; i<l.size(); i++) {
                Integer num = l.get(i);
                sum += num;

                if (num > max) {
                    max = num;
                }

                if (num < min) {
                    min = num;
                }
            }

            if (op == Op.SUM) {
                return sum;
            } else if (op == Op.MAX) {
                return max;
            } else if (op == Op.MIN) {
                return min;
            } else if (op == Op.AVG) {
                return sum / l.size();
            }

            return 0;
        }
    }

}
