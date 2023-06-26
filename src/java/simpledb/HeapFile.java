package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    // added fields
    private final File heapFile; // The file that stores the on-disk backing store for this heap file
    private final TupleDesc tpd; // TupleDesc of the table stored in this DbFile

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        // Constructor for HeapFile
        this.heapFile = f;
        this.tpd = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        // Returns the File backing this HeapFile on disk
        return this.heapFile;
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
        // throw new UnsupportedOperationException("implement this");
        // Returns an ID uniquely identifying this HeapFile
        return this.heapFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        // Returns the TupleDesc of the table stored in this DbFile
        return this.tpd;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // Reads a page with the specified PageId from the file
        if (pid.getPageNumber() < 0 || pid.getPageNumber() >= numPages()) {
            throw new IllegalArgumentException("Page does not exist in this file");
        }
        try {
            byte[] readData = new byte[BufferPool.getPageSize()];
            RandomAccessFile randomFile = new RandomAccessFile(this.heapFile,"r");
            randomFile.seek((long) BufferPool.getPageSize() * pid.getPageNumber());
            randomFile.read(readData);
            randomFile.close();
            return new HeapPage((HeapPageId) pid,readData);
        } catch (Exception e) {
            e.printStackTrace();
            throw new NoSuchElementException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        // Writes a page to the file
        int pageNumber = page.getId().getPageNumber();
        RandomAccessFile randomAccessFile = new RandomAccessFile(heapFile, "rw");
        randomAccessFile.seek((long) BufferPool.getPageSize() * pageNumber);
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // Returns the number of pages in this HeapFile
        return (int) Math.ceil((double)this.heapFile.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // return null;
        // not necessary for lab1
        // Inserts a tuple into the HeapFile
        ArrayList<Page> affectedPages = new ArrayList<>();
        boolean inserted = false;

        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                affectedPages.add(page);
                inserted = true;
                break;
            }
        }

        if (!inserted) {
            HeapPageId newPid = new HeapPageId(getId(), numPages());
            HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
            newPage.insertTuple(t);
            writePage(newPage);
            affectedPages.add(newPage);
        }

        return affectedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // return null;
        // not necessary for lab1
        // Deletes a tuple from the HeapFile
        ArrayList<Page> affectedPages = new ArrayList<>();
        HeapPageId pid = (HeapPageId) t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        affectedPages.add(page);
        return affectedPages;

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int currentPageNum = 0;
            private Iterator<Tuple> currentTupIter = null;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                HeapPageId pId = new HeapPageId(getId(),0);
                HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid,pId, Permissions.READ_ONLY);
                currentPageNum = 0;
                currentTupIter = hPage.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {

                if (currentTupIter == null) {
                    return false;
                }
                while (!currentTupIter.hasNext()) {
                    currentPageNum++;
                    if (currentPageNum >= numPages()) {
                        return false;
                    }
                    HeapPageId pId = new HeapPageId(getId(),currentPageNum);
                    HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid,pId,Permissions.READ_ONLY);
                    currentTupIter = hPage.iterator();
                }
                return true;

            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException("There are no more tuples");
                }
                return currentTupIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                currentTupIter = null;
                currentPageNum = 0;
            }
        };

    }

}

