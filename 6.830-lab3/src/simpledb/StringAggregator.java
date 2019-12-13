package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;

    private int afield;
    private Op what;

    private Map<Field, Integer> map;
    private int count;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) throw new IllegalArgumentException();

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        if (gbfield != NO_GROUPING) {
            map = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            count += 1;
        } else {
            Field field = tup.getField(gbfield);
            map.put(field, map.getOrDefault(field, 0) + 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {

        if (gbfield == NO_GROUPING) {
            TupleDesc td = new TupleDesc(1);
            td.setAttrNames(0, Aggregate.aggName(what));
            td.setTypes(0, Type.INT_TYPE);

            List<Tuple> tuples = new ArrayList<>();
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(count));
            tuples.add(tuple);
            return new TupleIterator(td, tuples);
        } else {
            TupleDesc td = new TupleDesc(2);
            td.setTypes(0, gbfieldtype);
            td.setTypes(1, Type.INT_TYPE);
            td.setAttrNames(1, Aggregate.aggName(what));

            List<Tuple> tuples = new ArrayList<>();
            for (Map.Entry<Field, Integer> entry : map.entrySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
                tuples.add(tuple);
            }
            return new TupleIterator(td, tuples);
        }

    }

}
