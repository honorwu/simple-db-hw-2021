package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    HashMap<Field, ArrayList<String>> group;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;

        group = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field k = tup.getField(gbfield);
        String v = ((StringField) tup.getField(afield)).getValue();

        if (!group.containsKey(k)) {
            group.put(k, new ArrayList<>());
        }

        group.get(k).add(v);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggIterator(group, gbfieldtype, op);
    }

    private class StringAggIterator implements OpIterator {
        private HashMap<Field, ArrayList<String>> group;
        private Type gbtype;
        private Op op;
        private Iterator iter;

        public StringAggIterator(HashMap<Field, ArrayList<String>> group, Type gbtype, Op op) {
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
            TupleDesc td = getTupleDesc();
            Tuple t = new Tuple(td);

            Map.Entry<Field, ArrayList<String>> e = (Map.Entry<Field, ArrayList<String>>) iter.next();

            t.setField(0, e.getKey());

            if (op == Op.COUNT) {
                t.setField(1, new IntField(e.getValue().size()));
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
    }
}
