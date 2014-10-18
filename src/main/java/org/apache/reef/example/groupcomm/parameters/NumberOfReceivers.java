/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.example.groupcomm.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 *
 */
@NamedParameter(doc = "The number of receivers for the operators")
public class NumberOfReceivers implements Name<Integer> {

}
