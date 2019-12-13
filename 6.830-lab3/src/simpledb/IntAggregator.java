package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;

    private int afield;
    private Op what;

    private Map<Field, Object> map;

    private int acc;
    private int helper;

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
        IntField field = (IntField) tup.getField(afield);
        if (gbfield == NO_GROUPING) {
            switch (what) {
                case MIN:
                    if (field.getValue() < acc) {
                        acc = field.getValue();
                    }
                    break;
                case MAX:
                    if (field.getValue() > acc) {
                        acc = field.getValue();
                    }
                    break;
                case COUNT:
                    acc += 1;
                    break;
                case AVG:
                    acc += field.getValue();
                    helper += 1;
                    break;
                case SUM:
                    acc += field.getValue();
            }
        } else {
            Field gb = tup.getField(gbfield);
            switch (what) {
                case MIN:
                    if (!map.containsKey(gb)) map.put(gb, field.getValue());
                    else {
                        int curmin = (int) map.get(gb);
                        if (field.getValue() < curmin) {
                            map.put(gb, field.getValue());
                        }
                    }
                    break;
                case MAX:
                    if (!map.containsKey(gb)) map.put(gb, field.getValue());
                    else {
                        int curmax = (int) map.get(gb);
                        if (curmax < field.getValue()) {
                            map.put(gb, field.getValue());
                        }
                    }
                    break;
                case COUNT:
                    map.put(gb, (int)map.getOrDefault(gb, 0) + 1);
                    break;
                case SUM:
                    map.put(gb, (int)map.getOrDefault(gb, 0) + field.getValue());
                    break;
                case AVG:
                    if (!map.containsKey(gb)) map.put(gb, new Pair(field.getValue(), 1));
                    else {
                        map.put(gb, new Pair(field.getValue() + ((Pair)map.get(gb)).getTotal(), ((Pair) map.get(gb)).getNum()+1));
                    }
            }
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
            td.setTypes(0, Type.INT_TYPE);
            td.setAttrNames(0, Aggregate.aggName(what));

            List<Tuple> tuples = new ArrayList<>();
            Tuple tuple = new Tuple(td);
            if (what == Op.AVG) {
                tuple.setField(0, new IntField(acc/helper));
            } else {
                tuple.setField(0, new IntField(acc));
            }
            tuples.add(tuple);

            return new TupleIterator(td, tuples);
        }

        TupleDesc td = new TupleDesc(2);
        td.setTypes(0, gbfieldtype);
        td.setAttrNames(1, Aggregate.aggName(what));
        td.setTypes(1, Type.INT_TYPE);

        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Object> entry : map.entrySet()) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, entry.getKey());
            if (what == Op.AVG) {
                tuple.setField(1, new IntField(((Pair)entry.getValue()).getTotal()/((Pair)entry.getValue()).getNum()));
            } else {
                tuple.setField(1, new IntField((Integer) entry.getValue()));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}

class Pair {
    private int total;
    private int num;

    public Pair(int total, int num) {
        this.total = total;
        this.num = num;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
