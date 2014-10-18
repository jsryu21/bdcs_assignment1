/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.example.groupcomm.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * The dimensionality of the model learned.
 */
@NamedParameter(doc = "Model dimensions", short_name = "dim")
public class ModelDimensions implements Name<Integer> {

}
