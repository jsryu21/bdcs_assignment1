/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.ml.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * The dimensionality of the model learned.
 */
@NamedParameter(doc = "Feature dimensions", short_name = "fDim")
public class Dimension implements Name<Integer> {

}
