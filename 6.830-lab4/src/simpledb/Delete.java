package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    private int count;
    private DbIterator iter;
    private TupleDesc td;

    public Delete(TransactionId t, DbIterator child) throws IOException, TransactionAbortedException, DbException {
        child.open();
        while (child.hasNext()) {
            Database.getBufferPool().deleteTuple(t, child.next());
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException, IOException {
        if (iter.hasNext()) {
            return iter.next();
        }
        return null;
    }
}
