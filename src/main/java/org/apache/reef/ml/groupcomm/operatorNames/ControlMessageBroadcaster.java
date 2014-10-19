/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.ml.groupcomm.operatorNames;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * Used to identify the broadcast operator for control flow messages.
 */
@NamedParameter()
public final class ControlMessageBroadcaster implements Name<String> {
}