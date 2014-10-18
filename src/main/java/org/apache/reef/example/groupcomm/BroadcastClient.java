package org.apache.reef.example.groupcomm;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.io.network.nggroup.impl.driver.GroupCommService;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.CommandLine;
import org.apache.reef.example.groupcomm.parameters.ModelDimensions;
import org.apache.reef.example.groupcomm.parameters.NumberOfReceivers;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for BGD job.
 */
@ClientSide
public class BroadcastClient {

  private static final Logger LOG = Logger.getLogger(BroadcastClient.class.getName());

  private static final int NUM_LOCAL_THREADS = 20;

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

  private static boolean isLocal;
  private static int jobTimeout;
  private static int dimensions;
  private static int numberOfReceivers;

  private static Configuration parseCommandLine(final String[] aArgs) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(Local.class);
      cl.registerShortNameOfClass(ModelDimensions.class);
      cl.registerShortNameOfClass(NumberOfReceivers.class);
      cl.processCommandLine(aArgs);
    } catch (final BindException | IOException ex) {
      final String msg = "Unable to parse command line";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
    return cb.build();
  }

  /**
   * copy the parameters from the command line required for the Client configuration
   */
  private static void storeCommandLineArgs(final Configuration commandLineConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    isLocal = injector.getNamedInstance(Local.class);
    jobTimeout = injector.getNamedInstance(TimeOut.class);
    dimensions = injector.getNamedInstance(ModelDimensions.class);
    numberOfReceivers = injector.getNamedInstance(NumberOfReceivers.class);
  }

  private static Configuration getRuntimeConfiguration() {
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
    return null;
  }

  private static LauncherStatus runBroadcastReef(final Configuration runtimeConfiguration, final int jobTimeout)
      throws InjectionException {

    final Configuration driverConfiguration = EnvironmentUtils
        .addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
        .set(DriverConfiguration.ON_DRIVER_STARTED, BroadcastDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, BroadcastDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, BroadcastDriver.ContextActiveHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, BroadcastDriver.ContextCloseHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, BroadcastDriver.FailedTaskHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "BroadcastDriver")
        .build();

    final Configuration groupCommServConfiguration = GroupCommService.getConfiguration();

    final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(groupCommServConfiguration, driverConfiguration)
        .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
        .bindNamedParameter(NumberOfReceivers.class, Integer.toString(numberOfReceivers))
        .build();

    LOG.info(new AvroConfigurationSerializer().toString(mergedDriverConfiguration));

    return DriverLauncher.getLauncher(runtimeConfiguration).run(mergedDriverConfiguration, jobTimeout);
  }


  public static void main(final String[] args) throws InjectionException {
    final Configuration commandLineConf = parseCommandLine(args);
    storeCommandLineArgs(commandLineConf);
    final Configuration runtimeConfiguration = getRuntimeConfiguration();
    final LauncherStatus status = runBroadcastReef(runtimeConfiguration, jobTimeout);

    LOG.log(Level.INFO, "REEF job finished: {0}", status);
  }
}
