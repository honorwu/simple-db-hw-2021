package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        RandomAccessFile f = null;
        try {
            int pageSize = Database.getBufferPool().getPageSize();
            int offset = pid.getPageNumber() * pageSize;

            f = new RandomAccessFile(file, "r");
            f.seek(offset);

            byte[] data = new byte[pageSize];

            f.read(data, 0, pageSize);

            HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());

            HeapPage page = new HeapPage(heapPageId, data);

            return page;
        } catch (IOException e) {

        } finally {
            try {
                f.close();
            } catch (IOException e) {

            }

        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = Database.getBufferPool().getPageSize();
        return (int) ((file.length() + pageSize - 1) / pageSize);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIteratorHelper iter = new DbFileIteratorHelper(tid, getId(), numPages());
        return iter;
    }

    private class DbFileIteratorHelper implements DbFileIterator {

        private TransactionId tid;
        private int tableId;
        private int pageNo;
        private int nextPageNo;
        Iterator<Tuple> tuples;
        boolean isOpen;

        DbFileIteratorHelper(TransactionId tid, int tableId, int pageNo) {
            this.tid = tid;
            this.tableId = tableId;
            this.pageNo = pageNo;
            nextPageNo = -1;
            isOpen = false;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                return false;
            }

            while (nextPageNo < pageNo) {
                if (tuples == null || !tuples.hasNext()) {
                    nextPageNo++;

                    if (nextPageNo >= pageNo) {
                        return false;
                    }

                    HeapPageId pageId = new HeapPageId(tableId, nextPageNo);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                    tuples = page.iterator();
                }

                if (tuples.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                throw new NoSuchElementException();
            }

            if (tuples == null || !tuples.hasNext()) {
                nextPageNo++;
                if (nextPageNo >= pageNo) {
                    return null;
                }

                HeapPageId pageId = new HeapPageId(tableId, nextPageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                tuples = page.iterator();
            }

            return tuples.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            nextPageNo = -1;
            tuples = null;
            isOpen = false;
        }
    }
}

