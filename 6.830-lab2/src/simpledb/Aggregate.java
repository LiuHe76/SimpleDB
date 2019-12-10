package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    private DbIterator iter;

    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) throws IOException, TransactionAbortedException, DbException {
        Aggregator aggregator;
        if (child.getTupleDesc().getType(afield) == Type.INT_TYPE) {
            if (gfield == -1) {
                aggregator = new IntAggregator(gfield, null, afield, aop);
            } else {
                aggregator = new IntAggregator(gfield, child.getTupleDesc().getType(gfield), afield, aop);
            }
        } else {
            if (gfield == -1) {
                aggregator = new StringAggregator(gfield, null, afield, aop);
            } else {
                aggregator = new StringAggregator(gfield, child.getTupleDesc().getType(gfield), afield, aop);
            }
        }

        child.open();
        while (child.hasNext()) {
            aggregator.merge(child.next());
        }
        child.close();

        iter = aggregator.iterator();
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
            throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
        iter.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException, IOException {
        if (iter.hasNext()) {
            return iter.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
        iter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        return iter.getTupleDesc();
    }

    public void close() {
        iter.close();
    }
}
