package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;


//    private int tableid;
    private int isCostPerPage;

    TupleDesc desc;
    private int tupleNum;
    private int pageNum;
    private int attrNum;
    private Map<Integer, Object> attrIndexToHist;

    private Object[] mins;
    private Object[] maxs;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.

//        this.tableid = tableid;
        this.isCostPerPage = ioCostPerPage;

        HeapFile file = (HeapFile) Database.getCatalog().getDbFile(tableid);
        desc = file.getTupleDesc();

        pageNum = file.numPages();
        attrNum = desc.numFields();
        mins = new Object[attrNum];
        maxs = new Object[attrNum];
        attrIndexToHist = new HashMap<>();

        TransactionId tid = new TransactionId();
        SeqScan seq = new SeqScan(tid, tableid, "");
        try {
            seq.open();

            while (seq.hasNext()) {
                Tuple tuple = seq.next();
                tupleNum += 1;
                for (int i = 0; i < attrNum; i += 1) {
                    Field field = tuple.getField(i);
                    if (mins[i] == null) {
                        mins[i] = field;
                    } else {
                        if (field.compare(Predicate.Op.LESS_THAN, (Field) mins[i])) {
                            mins[i] = field;
                        }
                    }
                    if (maxs[i] == null) {
                        maxs[i] = field;
                    } else {
                        if (field.compare(Predicate.Op.GREATER_THAN, (Field) maxs[i])) {
                            maxs[i] = field;
                        }
                    }
                }
            }

            for (int i = 0; i < attrNum; i += 1) {
                if (desc.getType(i) == Type.INT_TYPE) {
                    attrIndexToHist.put(i, new IntHistogram(NUM_HIST_BINS, ((IntField) mins[i]).getValue(), ((IntField) maxs[i]).getValue()));
                } else {
                    attrIndexToHist.put(i, new StringHistogram(NUM_HIST_BINS));
                }
            }

            seq.rewind();

            while (seq.hasNext()) {
                Tuple tuple = seq.next();
                for (int i = 0; i < attrNum; i += 1) {
                    Field field = tuple.getField(i);
                    if (desc.getType(i) == Type.INT_TYPE) {
                        ((IntHistogram) attrIndexToHist.get(i)).addValue(((IntField) field).getValue());
                    } else {
                        ((StringHistogram) attrIndexToHist.get(i)).addValue(((StringField) field).getValue());
                    }
                }
            }
            seq.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (DbException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	return pageNum * isCostPerPage;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	return (int) (tupleNum * selectivityFactor);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (desc.getType(field) == Type.INT_TYPE) {
            IntHistogram hist = (IntHistogram) attrIndexToHist.get(field);
            return hist.estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            StringHistogram hist = (StringHistogram) attrIndexToHist.get(field);
            return hist.estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

}
