/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.ml.math;

import java.io.Serializable;

/**
 * An interface for Linear Alebra Vectors.
 */
public interface Vector extends ImmutableVector, Serializable {

  /**
   * Set dimension i of the Vector to value v
   *
   * @param i the index
   * @param v value
   */
  public void set(final int i, final double v);

  /**
   * Adds the Vector that to this one in place: this += that.
   *
   * @param that
   */
  public void add(final Vector that);

  /**
   * this += factor * that.
   *
   * @param factor
   * @param that
   */
  public void multAdd(final double factor, final ImmutableVector that);

  /**
   * Scales this Vector: this *= factor.
   *
   * @param factor the scaling factor.
   */
  public void scale(final double factor);


  /**
   * Normalizes the Vector according to the L2 norm.
   */
  public void normalize();

}