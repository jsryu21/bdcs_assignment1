package org.apache.reef.ml.als;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Alternating Least Squares ML.
 */
public final class AlsClient {

  private static final Logger LOG = Logger.getLogger(AlsClient.class.getName());

  private static final int NUM_LOCAL_THREADS = 10;
  private static final int NUM_SPLITS = 2;

  /**
   * Command line parameters
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of minutes before timeout",
      short_name = "timeout", default_value = "2")
  public static final class TimeOut implements Name<Integer> {
  }

  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
  }

  /**
   * Start ALS job.
   * @param args
   */
  public static void main(final String[] args)
      throws InjectionException, IOException {

    LOG.log(Level.INFO, "REEF job started");

    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    new CommandLine(cb)
        .registerShortNameOfClass(Local.class)
        .registerShortNameOfClass(TimeOut.class)
        .registerShortNameOfClass(InputDir.class)
        .processCommandLine(args);

    final Injector injector = tang.newInjector(cb.build());

    final boolean isLocal = injector.getNamedInstance(Local.class);
    final int jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    final String inputDir = injector.getNamedInstance(InputDir.class);

    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running ALS on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running ALS on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    final Configuration dataLoadConfiguration = new DataLoadingRequestBuilder()
        .setMemoryMB(1024)
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(inputDir)
        .setNumberOfDesiredSplits(NUM_SPLITS)
        .setDriverConfigurationModule(DriverConfiguration.CONF
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "AlsDriver")
            .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(AlsDriver.class))
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, AlsDriver.ContextActiveHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, AlsDriver.TaskCompletedHandler.class))
        .build();

    final LauncherStatus status =
        DriverLauncher.getLauncher(runtimeConfiguration).run(dataLoadConfiguration, jobTimeout);

    LOG.log(Level.INFO, "REEF job finished: {0}", status);
  }
}
