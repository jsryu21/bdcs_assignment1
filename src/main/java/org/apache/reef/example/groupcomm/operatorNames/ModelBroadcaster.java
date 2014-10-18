/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.example.groupcomm.operatorNames;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * The name of the broadcast operator used for model broadcasts.
 */
@NamedParameter()
public final class ModelBroadcaster implements Name<String> {
}