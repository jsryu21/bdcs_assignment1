/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.example.groupcomm;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;

import javax.inject.Inject;

/**
 *
 */
public class ModelReceiveAckReduceFunction implements ReduceFunction<Boolean> {

  @Inject
  public ModelReceiveAckReduceFunction() {
  }

  @Override
  public Boolean apply(final Iterable<Boolean> elements) {
    return true;
  }

}
