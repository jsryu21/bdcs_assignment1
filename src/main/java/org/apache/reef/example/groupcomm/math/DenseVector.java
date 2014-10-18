/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.example.groupcomm.math;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

/**
 * A dense {@link Vector} implementation backed by a double[]
 */
public class DenseVector extends AbstractVector implements Serializable {

  private static final long serialVersionUID = 1L;
  private final double[] values;

  /**
   * Creates a dense vector of the given size
   */
  public DenseVector(final int size) {
    this(new double[size]);
  }

  public DenseVector(final double[] values) {
    this.values = values;
  }

  /**
   * Instantiates a new DenseVector by copying the given other vector.
   */
  public DenseVector(final ImmutableVector other) {
    final int size = other.size();
    this.values = new double[size];
    for (int i = 0; i < size; ++i) {
      this.values[i] = other.get(i);
    }
  }

  public DenseVector(final DenseVector other) {
    this.values = Arrays.copyOf(other.values, other.values.length);
  }

  @Override
  public void set(final int i, final double v) {
    this.values[i] = v;
  }

  @Override
  public double get(final int i) {
    return this.values[i];
  }

  @Override
  public int size() {
    return this.values.length;
  }

  /**
   * Access the underlying storage. This is unsafe.
   */
  public double[] getValues() {
    return this.values;
  }

  /**
   * Creates a random Vector of size 'size' where each element is individually
   * drawn from Math.random()
   *
   * @return a random Vector of the given size where each element is
   * individually drawn from Math.random()
   */
  public static DenseVector rand(final int size) {
    return rand(size, new Random());
  }

  /**
   * Creates a random Vector of size 'size' where each element is individually
   * drawn from Math.random()
   *
   * @param random the random number generator to use.
   * @return a random Vector of the given size where each element is
   * individually drawn from Math.random()
   */
  public static DenseVector rand(final int size, final Random random) {
    final DenseVector vec = new DenseVector(size);
    for (int i = 0; i < size; ++i) {
      vec.values[i] = random.nextDouble();
    }
    return vec;
  }
}
