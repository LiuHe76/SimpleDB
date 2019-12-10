package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    private int count;
    private TupleDesc td;
    private DbIterator iter;

    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException, IOException, TransactionAbortedException {
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))) throw new DbException("TupleDesc of child differs from table into which are to be inserted.");

        child.open();
        while (child.hasNext()) {
            Database.getBufferPool().insertTuple(t, tableid, child.next());
            count += 1;
        }
        child.close();

        td = new TupleDesc(1);
        td.setTypes(0, Type.INT_TYPE);
        Tuple tuple = new Tuple(td);
        tuple.setField(0, new IntField(count));
        List<Tuple> tuples = new ArrayList<>();
        tuples.add(tuple);
        iter = new TupleIterator(td, tuples);
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException, IOException {
        iter.open();
    }

    public void close() {
        iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
        iter.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException, IOException {
        if (iter.hasNext()) {
            return iter.next();
        }
        return null;
    }
}
