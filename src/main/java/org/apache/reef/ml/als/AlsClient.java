package org.apache.reef.ml.als;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.io.network.nggroup.impl.driver.GroupCommService;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.*;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.tang.formats.ConfigurationModule;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.ml.parameters.*;

import javax.inject.Inject;
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

  private final boolean isLocal;
  private final int timeout;
  private final String inputDir;
  private final int evalSize;
  private final int splitNum;
  private final AlsParameters alsParameters;

  @Inject
  private AlsClient(@Parameter(Local.class) final boolean isLocal,
                    @Parameter(Timeout.class) final int timeout,
                    @Parameter(InputDir.class) final String inputDir,
                    @Parameter(EvalSize.class) final int evalSize,
                    @Parameter(SplitNum.class) final int splitNum,
                    AlsParameters alsParameters) {
    this.isLocal = isLocal;
    this.timeout = timeout;
    this.inputDir = inputDir;
    this.evalSize = evalSize;
    this.splitNum = splitNum;
    this.alsParameters = alsParameters;
  }

  private static AlsClient parseCommandLine(final String[] args)
      throws IOException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(Local.class);
    cl.registerShortNameOfClass(TimeOut.class);
    cl.registerShortNameOfClass(InputDir.class);
    cl.registerShortNameOfClass(EvalSize.class);
    cl.registerShortNameOfClass(SplitNum.class);
    AlsParameters.registerShortNameOfClass(cl);

    cl.processCommandLine(args);

    return Tang.Factory.getTang().newInjector(cb.build()).getInstance(AlsClient.class);
  }

  private Configuration getRuntimeConfiguration() {
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

  private LauncherStatus run() throws InjectionException {
    return DriverLauncher.getLauncher(getRuntimeConfiguration())
        .run(getDriverConfWithDataLoad(), timeout);
  }

  private Configuration getDriverConfWithDataLoad() {

    final ConfigurationModule alsDriverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "AlsDriver")
        .set(DriverConfiguration.GLOBAL_FILES, EnvironmentUtils.getClassLocation(AlsDriver.class))
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, AlsDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, AlsDriver.TaskCompletedHandler.class);

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

    return Configurations.merge(alsDriverConfWithDataLoad,
        GroupCommService.getConfiguration(),
        alsParameters.getDriverConfiguration());
  }

  public static void main(final String[] args) throws InjectionException, IOException {

    LOG.log(Level.INFO, "REEF job started");

    AlsClient alsClient = parseCommandLine(args);
    final LauncherStatus status = alsClient.run();

    LOG.log(Level.INFO, "REEF job finished: {0}", status);
  }
}
