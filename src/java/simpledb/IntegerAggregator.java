package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, IntegerAggregate> aggregateResults;

    // IDEA: We will have a separate class to calculate the AVG of the tuples
    // And for that we only need the tuple aggregated value and tuple count
    // In this case we will use this class inside our Helper Method
    // that will calculate the AVG
    private static class IntegerAggregate {
        public int count;
        public int intAggregateValue;
    };

    /**
     * Helper Method : CalculateAvg essentially calculates the average separately
     * It only gathers the MergeTuple info for SUM and COUNT. Then calculates AVG
     * My thought: I wanted to calculate everything inside the merge tuple and then have
     * NO GROUPING inside the OpIterator where I will calculate the AVG once again.
     * It WORKED for just IntegerAggregatorTest
     * BUT didn't work for Aggregator SYSTEM TESTS :(
     */
    public int CalculateAvg(IntegerAggregate ig) {
        switch (what) {
            case MIN:
            case MAX:
            case SUM:
                return ig.intAggregateValue;
            case COUNT:
                return ig.count;
            case AVG:
                if (ig.count != 0)
                    return ig.intAggregateValue/ig.count;
                return 0; // to not have 0 as denominator which will give us error
            default:
                throw new UnsupportedOperationException("This is not a valid aggregation operation.");
        }
    }


    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregateResults = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field groupField = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        IntegerAggregate ig = aggregateResults.get(groupField);
        int aggregateVal = ((IntField) tup.getField(afield)).getValue();

        // if integerAggregator is null, we reset everything
        if (ig == null) {
            ig = new IntegerAggregate();
            ig.count = 1;
            ig.intAggregateValue = aggregateVal;
        } else {
            switch (what) {
                case MIN:
                    ig.intAggregateValue = Math.min(aggregateVal, ig.intAggregateValue);
                    break;
                case MAX:
                    ig.intAggregateValue = Math.max(aggregateVal, ig.intAggregateValue);
                    break;
                case AVG:
                case COUNT:
                    ig.count++;
                case SUM:
                    ig.intAggregateValue += aggregateVal;
                    break;
                default:
                    throw new UnsupportedOperationException("This is not a valid aggregation operation.");
            }
        }
        aggregateResults.put(groupField, ig);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");

        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});

        // Now comes the NO_GROUPING AVG calculation
        for (Entry<Field, IntegerAggregate> entry : aggregateResults.entrySet()) {
            if (gbfield == NO_GROUPING) {
                tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, new IntField(CalculateAvg(aggregateResults.get(null))));
                tuples.add(tuple);
            } else {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(CalculateAvg(entry.getValue())));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}

