/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package org.apache.reef.ml.als;

import java.io.Serializable;

public enum ControlMessages implements Serializable {
  ReceiveModel,
  Stop
}