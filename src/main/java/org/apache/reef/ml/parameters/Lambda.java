package org.apache.reef.ml.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * Lambda of model.
 */
@NamedParameter(doc = "Weighted-lambda-regularization", short_name = "lambda")
public class Lambda implements Name<Double> {

}