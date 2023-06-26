package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid; // The transaction running the insert
    private OpIterator child; // The child operator from which to read tuples to be inserted
    private final int tableId; // The table in which to insert tuples
    private boolean inserted; // Flag to indicate if tuples have been inserted

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        // Initialize inserted flag to false
        this.inserted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        // return null;
        // Return a TupleDesc with a single integer field for the number of inserted records
        return new TupleDesc(new  Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        // Open the child operator and call the superclass open method
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        // Close the child operator and call the superclass close method
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        // Rewind the child operator
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // return null;
        // Insert tuples read from the child operator into the tableId
        // and return a 1-field tuple with the count of inserted records
        if (inserted) {
            return null;
        }

        int count = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
                count++;
            } catch (Exception e) {
                throw new DbException("The insert has failed");
            }
        }
        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, new IntField(count));
        // Set inserted flag to true after insertion
        inserted = true;
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        // return null;
        // Return an array containing the child operator
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        // Set the child operator if there is exactly one child in the input array
        if (children.length == 1) {
            child = children[0];
        }
    }
}
