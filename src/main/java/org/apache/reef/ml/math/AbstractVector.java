/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.ml.math;

/**
 * Abstract base class for {@link Vector} implementations.
 * <p/>
 * The only methods to be implemented by subclasses are get, set and size.
 */
public abstract class AbstractVector extends AbstractImmutableVector implements Vector {

  @Override
  public abstract void set(int i, double v);


  @Override
  public void add(final Vector that) {
    for (int index = 0; index < this.size(); ++index) {
      this.set(index, this.get(index) + that.get(index));
    }
  }

  @Override
  public void multAdd(final double factor, final ImmutableVector that) {
    for (int index = 0; index < this.size(); ++index) {
      this.set(index, this.get(index) + factor * that.get(index));
    }
  }

  @Override
  public void scale(final double factor) {
    for (int index = 0; index < this.size(); ++index) {
      this.set(index, this.get(index) * factor);
    }
  }


  @Override
  public void normalize() {
    final double factor = 1.0 / this.norm2();
    this.scale(factor);
  }


}
