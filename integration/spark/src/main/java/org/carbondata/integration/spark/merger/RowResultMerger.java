package org.carbondata.integration.spark.merger;

import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.schema.metadata.CarbonColumnarFactMergerInfo;
import org.carbondata.processing.store.CarbonFactDataHandlerColumnarMerger;
import org.carbondata.processing.store.CarbonFactHandler;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

import java.util.*;

/**
 *
 */
public class RowResultMerger {

    CarbonFactHandler dataHandler;
    private List<CarbonIterator<Result>> blockIteratorList = new ArrayList<CarbonIterator<Result>>
            (CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    private SegmentProperties segprop;
    /**
     * record holder heap
     */
    private AbstractQueue<CarbonIterator<Result>> recordHolderHeap;

    private TupleConversionAdapter tupleConvertor;

    public RowResultMerger(List<CarbonIterator<Result>> iteratorList,
                           String cubeName,
                           String schemaName,
                           String loadPath,
                           int[] dimlens, int mdKeyLength, String tableName,
                           int currentRestructNumber, SegmentProperties segProp) {
        this.blockIteratorList = iteratorList;
        recordHolderHeap = new PriorityQueue<CarbonIterator<Result>>(
                blockIteratorList.size(), new RowResultMerger.CarbonMdkeyComparator());

        this.segprop = segProp;
        CarbonColumnarFactMergerInfo carbonColumnarFactMergerInfo =
                new CarbonColumnarFactMergerInfo();
        carbonColumnarFactMergerInfo.setCubeName(cubeName);
        carbonColumnarFactMergerInfo.setSchemaName(schemaName);
        carbonColumnarFactMergerInfo.setDestinationLocation(loadPath);
        carbonColumnarFactMergerInfo.setDimLens(dimlens);
        carbonColumnarFactMergerInfo.setMdkeyLength(mdKeyLength);
        carbonColumnarFactMergerInfo.setTableName(tableName);
        carbonColumnarFactMergerInfo.setType(new char['n']);
        carbonColumnarFactMergerInfo.setMeasureCount(segProp.getMeasures().size());
        carbonColumnarFactMergerInfo.setIsUpdateFact(false);
        carbonColumnarFactMergerInfo.setGlobalKeyGen(segProp.getDimensionKeyGenerator());
        dataHandler = new CarbonFactDataHandlerColumnarMerger(carbonColumnarFactMergerInfo,
                currentRestructNumber);

        tupleConvertor = new TupleConversionAdapter(segProp);
    }

    public void mergerSlice() throws SliceMergerException {
        int index = 0;
        try {

            dataHandler.initialise();

            // add all iterators to the queue
            for (CarbonIterator<Result> leaftTupleIterator : this
                    .blockIteratorList) {
                this.recordHolderHeap.add(leaftTupleIterator);
                index++;
            }
            CarbonIterator<Result> poll = null;
            while (index > 1) {
                // poll the top record
                poll = this.recordHolderHeap.poll();
                // get the mdkey
                addRow(poll.next());
                // if there is no record in the leaf and all then decrement the
                // index
                if (!poll.hasNext()) {
                    index--;
                    continue;
                }
                // poll.fetchNextData();
                // add record to heap
                this.recordHolderHeap.add(poll);
            }
            // if record holder is not empty then poll the slice holder from
            // heap
            poll = this.recordHolderHeap.poll();
            while (true) {
                addRow(poll.next());
                // check if leaf contains no record
                if (!poll.hasNext()) {
                    break;
                }
                //   poll.fetchNextData();
            }
            this.dataHandler.finish();

        } catch (CarbonDataWriterException e) {
            throw new SliceMergerException(
                    "Problem while getting the file channel for Destination file: ", e);
        } finally {
            this.dataHandler.closeHandler();
        }
    }

    /**
     * Below method will be used to add sorted row
     *
     * @throws SliceMergerException
     */
    protected void addRow(Result carbonTuple) throws SliceMergerException {
        Object[] row;

        row = tupleConvertor.getObjectArray(carbonTuple);

        try {
            this.dataHandler.addDataToStore(row);
        } catch (CarbonDataWriterException e) {
            throw new SliceMergerException("Problem in merging the slice", e);
        }
    }

    private class CarbonMdkeyComparator
            implements Comparator<CarbonIterator<Result>> {

        @Override
        public int compare(CarbonIterator<Result> o1,
                           CarbonIterator<Result> o2) {

            ByteArrayWrapper key1 = o1.next().getKey();
            ByteArrayWrapper key2 = o2.next().getKey();
            int compareResult = 0;
            int[] columnValueSizes = segprop.getEachDimColumnValueSize();
            int dictionaryKeyOffset = 0;
            byte[] dimCols1 = key1.getDictionaryKey();
            byte[] dimCols2 = key2.getDictionaryKey();
            int noDicIndex = 0;
            for (int eachColumnValueSize : columnValueSizes) {
                // case of dictionary cols
                if (eachColumnValueSize > 0) {

                    compareResult = ByteUtil.UnsafeComparer.INSTANCE
                            .compareTo(dimCols1, dictionaryKeyOffset, eachColumnValueSize,
                                    dimCols2, dictionaryKeyOffset, eachColumnValueSize);
                    dictionaryKeyOffset += eachColumnValueSize;
                } else { // case of no dictionary

                    byte[] noDictionaryDim1 = key1.getNoDictionaryKeyByIndex(noDicIndex);
                    byte[] noDictionaryDim2 = key2.getNoDictionaryKeyByIndex(noDicIndex);
                    compareResult = ByteUtil.UnsafeComparer.INSTANCE
                            .compareTo(noDictionaryDim1, noDictionaryDim2);
                    noDicIndex++;

                }
                if (0 != compareResult) {
                    return compareResult;
                }
            }
            return 0;
        }
    }

}
