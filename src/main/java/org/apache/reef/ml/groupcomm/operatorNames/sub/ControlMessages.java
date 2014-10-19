/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.ml.groupcomm.operatorNames.sub;

import java.io.Serializable;

public enum ControlMessages implements Serializable {
  IdentifyMU,
  ComputeM,
  ComputeU,
  Stop
}