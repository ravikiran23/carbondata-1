package org.carbondata.integration.spark.merger;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.processing.util.RemoveDictionaryUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.result.BatchRawResult;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * This class will be used to convert the Result into the format used in data write
 */
public class TupleConversionAdapter {

  private final SegmentProperties segmentproperties;

  private final List<CarbonMeasure> measureList;

  private int noDictionaryPresentIndex;

  private int measureCount;

  private boolean isNoDictionaryPresent;

  public TupleConversionAdapter(SegmentProperties segmentProperties) {
    this.measureCount = segmentProperties.getMeasures().size();
    this.isNoDictionaryPresent = segmentProperties.getNumberOfNoDictionaryDimension() > 0;
    if (isNoDictionaryPresent) {
      noDictionaryPresentIndex++;
    }
    this.segmentproperties = segmentProperties;
    measureList = segmentProperties.getMeasures();
  }

  public Object[] getObjectArray(Object[] carbonTuple) {
    Object[] row = new Object[measureCount + noDictionaryPresentIndex + 1];
    int index = 0;
//    MeasureAggregator[] measureAggregator = carbonTuple.getValue();
    // put measures.

    for(int j = 1; j <= measureCount ; j++){
      row[index++] = carbonTuple[j];
    }

    /*for (CarbonMeasure msr : measureList) {
      Object val;
      switch (msr.getDataType()) {
        case LONG:
          val = measureAggregator[index].getLongValue();
          break;
        case DECIMAL:
          val = measureAggregator[index].getBigDecimalValue();
          break;
        default:
          val = measureAggregator[index].getDoubleValue();
          break;
      }
      row[index++] = val;
    }*/

    // put No dictionary byte []
    if (isNoDictionaryPresent) {

      int noDicCount = segmentproperties.getNumberOfNoDictionaryDimension();
      List<byte[]> noDicByteArr = new ArrayList<>(noDicCount);
      for (int i = 0; i < noDicCount; i++) {
        noDicByteArr.add(((ByteArrayWrapper)carbonTuple[0]).getNoDictionaryKeyByIndex(i));
      }
      byte[] singleByteArr = RemoveDictionaryUtil.convertListByteArrToSingleArr(noDicByteArr);

      row[index++] = singleByteArr;
    }

    // put No Dictionary Dims
    row[index++] = ((ByteArrayWrapper)carbonTuple[0]).getDictionaryKey();
    return row;
  }
}
