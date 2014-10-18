package org.apache.reef.ml.als;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.InjectionException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Alternating Least Squares ML.
 */
public final class AlsClient {

  private static final Logger LOG = Logger.getLogger(AlsClient.class.getName());

  /**
   * @return the configuration of the ALS driver
   */
  private static Configuration getDriverConfiguration() {

    final Configuration driverConfiguration = DriverConfiguration.CONF
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "ALS")
      .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(AlsDriver.class))
      .set(DriverConfiguration.ON_DRIVER_STARTED, AlsDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, AlsDriver.EvaluatorAllocatedHandler.class)
      .build();

    return driverConfiguration;
  }

  private static LauncherStatus runALS(Configuration runtimeConfiguration) throws InjectionException {
    LOG.log(Level.INFO, "runALS called");
    final Configuration driverConf = getDriverConfiguration();

    // DriverLauncher launches Driver to run the application.
    DriverLauncher.getLauncher(runtimeConfiguration).run(driverConf);
    return LauncherStatus.COMPLETED;
  }

  /**
   * Start ALS job.
   * @param args
   */
  public static void main(final String[] args) throws InjectionException {
    LOG.log(Level.INFO, "REEF job started");
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
      .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 3).build();

    final LauncherStatus status = runALS(runtimeConfiguration);
    LOG.log(Level.INFO, "REEF job finished: {0}", status);
  }
}
