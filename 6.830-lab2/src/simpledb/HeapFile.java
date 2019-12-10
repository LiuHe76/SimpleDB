package simpledb;

import java.beans.Transient;
import java.io.*;
import java.sql.BatchUpdateException;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;
    private int pageNum;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        this.td = td;
        pageNum = (int) (file.length() / BufferPool.PAGE_SIZE);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IOException {
        if (pid.pageno() < numPages()) {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(BufferPool.PAGE_SIZE * pid.pageno());
            byte[] datas = new byte[BufferPool.PAGE_SIZE];
            int i = randomAccessFile.read(datas);
            assert i == BufferPool.PAGE_SIZE;
            randomAccessFile.close();
            return new HeapPage((HeapPageId) pid, datas);
        } else {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.seek(file.length());
            byte[] datas = new byte[BufferPool.PAGE_SIZE];
            randomAccessFile.write(datas);
            randomAccessFile.close();

            pageNum += 1;
            return new HeapPage((HeapPageId) pid, new byte[BufferPool.PAGE_SIZE]);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(page.getId().pageno() * BufferPool.PAGE_SIZE);
        randomAccessFile.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return pageNum;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        HeapPage page;
        int id = 0;
        while (true) {
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), id), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                break;
            } else {
                id += 1;
            }
        }

        page.addTuple(t);
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException, IOException {
        if (t.getRecordId().getPageId().getTableId() != getId()) throw new DbException("tuple not belong to current table.");
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), t.getRecordId().getPageId().pageno()), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return page;
    }

    private class FileIterator implements DbFileIterator {
        private TransactionId tid;

        private int pageid;
        private Iterator<Tuple> pageIter;
        private boolean isOpen;

        public FileIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException, IOException {
            isOpen = true;
            pageid = 0;
            pageIter = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageid++), Permissions.READ_ONLY)).iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException, IOException {
            if (!isOpen) return false;
            while (true) {
                if (pageIter.hasNext()) return true;
                if (pageid < pageNum) {
                    pageIter = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageid++), Permissions.READ_ONLY)).iterator();
                } else {
                    return false;
                }
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) throw new NoSuchElementException();
            return pageIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException, IOException {
            pageid = 0;
            pageIter = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageid++), Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void close() {
            isOpen = false;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new FileIterator(tid);
    }
    
}

