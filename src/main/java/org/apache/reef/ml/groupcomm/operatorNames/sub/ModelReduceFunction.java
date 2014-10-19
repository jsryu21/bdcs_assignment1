/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.ml.groupcomm.operatorNames.sub;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import org.apache.reef.ml.math.DenseVector;
import org.apache.reef.ml.math.Vector;

import javax.inject.Inject;
import java.util.ArrayList;

/**
 *
 */
public class ModelReduceFunction implements ReduceFunction<ArrayList> {

  @Inject
  public ModelReduceFunction() {
  }

  @Override
  public ArrayList apply(final Iterable<ArrayList> elements) {
    ArrayList<Vector> vectorList;
    for (final ArrayList<Vector> element : elements) {
      /*

      Aggregate all vector and return it to Controller.

       */

    }
    return null;
  }

}
