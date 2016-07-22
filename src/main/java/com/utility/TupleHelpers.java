package com.utility;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

/**
 * 辅助类.用于识别TickTuple
 * @author Dx
 *
 */
public final class TupleHelpers {

  private TupleHelpers() {
  }

  /**
   * 用于判断该Tuple是否为TickTuple
   * @param tuple
   * @return
   */
  //通过Tuple.getSourceComponent方法来判断来自哪个组件
  public static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
        Constants.SYSTEM_TICK_STREAM_ID);
  }

}
