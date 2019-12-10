package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
    private DbFileIterator dbFileIterator;
    private DbFile dbFile;
    private TransactionId tid;
    private TupleDesc aliaDesc;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        dbFile = Database.getCatalog().getDbFile(tableid);
        TupleDesc tupleDesc = dbFile.getTupleDesc();
        aliaDesc = new TupleDesc(tupleDesc.numFields());
        for (int i = 0; i < tupleDesc.numFields(); i += 1) {
            aliaDesc.setTypes(i, tupleDesc.getType(i));
            StringBuffer sb = new StringBuffer();
            sb.append(tableAlias).append(".").append(tupleDesc.getFieldName(i));
            aliaDesc.setAttrNames(i, sb.toString());
        }
    }

    public void open()
            throws DbException, TransactionAbortedException, IOException {
        dbFileIterator = dbFile.iterator(tid);
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return aliaDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException, IOException {
        return dbFileIterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        return dbFileIterator.next();
    }

    public void close() {
        dbFileIterator.close();
    }

    public void rewind()
            throws DbException, NoSuchElementException, TransactionAbortedException, IOException {
       dbFileIterator.rewind();
    }
}
