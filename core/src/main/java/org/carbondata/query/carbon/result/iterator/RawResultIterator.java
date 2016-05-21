package org.carbondata.query.carbon.result.iterator;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.result.BatchRawResult;

/**
 *
 */
public class RawResultIterator implements CarbonIterator<Object[]> {

  private CarbonIterator<BatchRawResult> detailRawQueryResultIterator;

  private int counter = 0;

  private BatchRawResult batch;

  public RawResultIterator(CarbonIterator<BatchRawResult> detailRawQueryResultIterator) {
    this.detailRawQueryResultIterator = detailRawQueryResultIterator;
  }

  @Override public boolean hasNext() {

    if (null == batch || checkIfBatchIsProcessedCompletely(batch)) {
      if (detailRawQueryResultIterator.hasNext()) {
        batch = detailRawQueryResultIterator.next();
        counter = 0; // batch changed so reset the counter.
      } else {
        return false;
      }
    }

    if (!checkIfBatchIsProcessedCompletely(batch)) {
      return true;
    } else {
      return false;
    }
  }

  @Override public Object[] next() {

    if (null == batch) { // for 1st time
      batch = detailRawQueryResultIterator.next();
    }
    if (!checkIfBatchIsProcessedCompletely(batch)) {
      return batch.getRawRow(counter++);
    } else { // completed one batch.
      batch = detailRawQueryResultIterator.next();
      counter = 0;
    }
    return batch.getRawRow(counter++);

  }

  /**
   * for fetching the row with out incrementing counter.
   * @return
   */
  public Object[] fetch(){
    if(hasNext())
    {
      return batch.getRawRow(counter);
    }
    else
    {
      return null;
    }
  }

  private boolean checkIfBatchIsProcessedCompletely(BatchRawResult batch){
    if(counter < batch.getSize())
    {
      return false;
    }
    else{
      return true;
    }
  }
}
