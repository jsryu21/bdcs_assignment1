/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.example.groupcomm.math;

import com.microsoft.reef.io.Tuple;

/**
 * Represents an immutable vector.
 */
public interface ImmutableVector {
  /**
   * Access the value of the Vector at dimension i
   *
   * @param i index
   * @return the value at index i
   */
  double get(int i);

  /**
   * The size (dimensionality) of the Vector
   *
   * @return the size of the Vector.
   */
  int size();

  /**
   * Computes the inner product with another Vector.
   *
   * @param that
   * @return the inner product between two Vectors.
   */
  double dot(Vector that);

  /**
   * Computes the computeSum of all entries in the Vector.
   *
   * @return the computeSum of all entries in this Vector
   */
  double sum();

  /**
   * Computes the L2 norm of this Vector.
   *
   * @return the L2 norm of this Vector.
   */
  double norm2();

  /**
   * Computes the square of the L2 norm of this Vector.
   *
   * @return the square of the L2 norm of this Vector.
   */
  double norm2Sqr();

  /**
   * Computes the min of all entries in the Vector
   *
   * @return the min of all entries in this Vector
   */
  Tuple<Integer, Double> min();
}