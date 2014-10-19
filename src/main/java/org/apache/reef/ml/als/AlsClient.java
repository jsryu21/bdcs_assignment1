package org.apache.reef.ml.als;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
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
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.tang.formats.ConfigurationModule;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.example.groupcomm.parameters.ModelDimensions;
import org.apache.reef.example.groupcomm.parameters.SplitNum;
import org.apache.reef.ml.parameters.*;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for ALS job.
 */
@ClientSide
public class AlsClient {

  private static final Logger LOG = Logger.getLogger(AlsClient.class.getName());

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

  private static String inputDir;
  private static int evalSize;
  private static int splitNum;

  private static double lambda;
  private static int dimensions;

  private static Configuration parseCommandLine(final String[] aArgs) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(Local.class);
      cl.registerShortNameOfClass(TimeOut.class);

      cl.registerShortNameOfClass(InputDir.class);
      cl.registerShortNameOfClass(EvalSize.class);
      cl.registerShortNameOfClass(SplitNum.class);

      cl.registerShortNameOfClass(Lambda.class);
      cl.registerShortNameOfClass(ModelDimensions.class);

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
    jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    inputDir = injector.getNamedInstance(InputDir.class);
    evalSize = injector.getNamedInstance(EvalSize.class);
    splitNum = injector.getNamedInstance(SplitNum.class);
    lambda = injector.getNamedInstance(Lambda.class);
    dimensions = injector.getNamedInstance(ModelDimensions.class);
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
    return runtimeConfiguration;
  }

  private static LauncherStatus runAlsReef(final Configuration runtimeConfiguration, final int jobTimeout)
      throws InjectionException {

    final ConfigurationModule alsDriverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "AlsDriver")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(AlsDriver.class))
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, AlsDriver.ContextActiveHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, AlsDriver.ContextCloseHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, AlsDriver.FailedTaskHandler.class);

    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(evalSize)
        .build();

    final Configuration alsDriverConfWithDataLoad = new DataLoadingRequestBuilder()
        .setMemoryMB(evalSize)
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(inputDir)
        .setNumberOfDesiredSplits(splitNum)
        .setComputeRequest(computeRequest)

        .setDriverConfigurationModule(alsDriverConfiguration)

        .build();

    final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(groupCommServConfiguration, driverConfiguration)
        .bindNamedParameter(Lambda.class, Double.toString(lambda))
        .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
        .bindNamedParameter(org.apache.reef.example.groupcomm.parameters.SplitNum.class, Integer.toString(splitNum))
        .build();

    LOG.info(new AvroConfigurationSerializer().toString(mergedDriverConfiguration));

    return DriverLauncher.getLauncher(runtimeConfiguration).run(mergedDriverConfiguration, jobTimeout);
  }

  public static void main(final String[] args) throws InjectionException {

    LOG.log(Level.INFO, "REEF job started");

    final Configuration commandLineConf = parseCommandLine(args);
    storeCommandLineArgs(commandLineConf);
    final Configuration runtimeConfiguration = getRuntimeConfiguration();
    final LauncherStatus status = runAlsReef(runtimeConfiguration, jobTimeout);

    LOG.log(Level.INFO, "REEF job finished: {0}", status);
  }
}
