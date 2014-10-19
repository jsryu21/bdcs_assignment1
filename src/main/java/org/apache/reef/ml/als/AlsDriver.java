package org.apache.reef.ml.als;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.network.nggroup.api.driver.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.driver.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;
import org.apache.reef.ml.groupcomm.operatorNames.sub.ModelReduceFunction;
import org.apache.reef.ml.groupcomm.operatorNames.ControlMessageBroadcaster;
import org.apache.reef.ml.groupcomm.operatorNames.ModelBroadcaster;
import org.apache.reef.ml.groupcomm.operatorNames.ModelReducer;
import org.apache.reef.ml.groupcomm.AllCommGroup;
import org.apache.reef.ml.parameters.AlsParameters;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver for Alternating Least Squares ML.
 */
@Unit
public final class AlsDriver {

  private static final Logger LOG = Logger.getLogger(AlsDriver.class.getName());

  private final AtomicBoolean masterTaskSubmitted = new AtomicBoolean(false);

  private final AtomicBoolean ctrlTaskPresent = new AtomicBoolean(false);
  private final AtomicInteger contextId = new AtomicInteger(0);
  private final AtomicInteger taskId = new AtomicInteger(0);
  private String ctrlTaskContextId;

  private final EvaluatorRequestor requestor;
  private final GroupCommDriver groupCommDriver;
  private final CommunicationGroupDriver allCommGroup;
  private final DataLoadingService dataLoadingService;
  private final AlsParameters alsParameters;

  /**
   * Job driver constructor - instantiated via TANG.
   */
  @Inject
  public AlsDriver(
      final EvaluatorRequestor requestor,
      final GroupCommDriver groupCommDriver,
      final DataLoadingService dataLoadingService,
      final AlsParameters alsParameters) {

    this.requestor = requestor;
    this.groupCommDriver = groupCommDriver;
    this.dataLoadingService = dataLoadingService;
    this.alsParameters = alsParameters;

    this.allCommGroup = this.groupCommDriver.newCommunicationGroup(
        AllCommGroup.class,
        dataLoadingService.getNumberOfPartitions() + 1);

    LOG.log(Level.INFO, "Obtained new communication group");

    this.allCommGroup
        .addBroadcast(ControlMessageBroadcaster.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(ControllerTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addBroadcast(ModelBroadcaster.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(ControllerTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addReduce(ModelReducer.class,
            ReduceOperatorSpec.newBuilder()
                .setReceiverId(ControllerTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .setReduceFunctionClass(ModelReduceFunction.class)
                .build())
        .finalise();

    LOG.log(Level.INFO, "Added operators to allCommGroup");
  }

  public class ActiveContextHandler implements EventHandler<ActiveContext> {

    private final AtomicBoolean storeCtrlId = new AtomicBoolean(false);

    @Override
    public void onNext(final ActiveContext activeContext) {

      final String ctxId = activeContext.getId();

      LOG.log(Level.INFO, "Got active context: {0}", ctxId);

      /**
       * The active context can be either from data loading service or after network
       * service has loaded contexts. So check if the GroupCommDriver knows if it was
       * configured by one of the communication groups.
       */
      if (groupCommDriver.isConfigured(activeContext)) {
        final Configuration partialTaskConf;

        if (ctxId.equals(ctrlTaskContextId) && !masterTaskSubmitted()) {
          LOG.log(Level.INFO, "Submit ControllerTask");
          partialTaskConf = Configurations.merge(
              TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, ControllerTask.TASK_ID)
                  .set(TaskConfiguration.TASK, ControllerTask.class)
                  .build(),
              alsParameters.getCtrlTaskConfiguration()
          );

        } else {

          LOG.log(Level.INFO, "Submit ComputeTask");
          partialTaskConf = Configurations.merge(
              TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, getSlaveId())
                  .set(TaskConfiguration.TASK, ComputeTask.class)
                  .build(),
              alsParameters.getCompTaskConfiguration()
          );

        }

        allCommGroup.addTask(partialTaskConf);

        final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);

        activeContext.submitTask(taskConf);

      } else {

        final Configuration contextConf = groupCommDriver.getContextConfiguration();
        final Configuration serviceConf = groupCommDriver.getServiceConfiguration();

        if (dataLoadingService.isComputeContext(activeContext)) {
          LOG.log(Level.INFO, "Submitting GroupCommContext for ControllerNode to underlying context");
          final String contextId = contextId(contextConf);

          if (storeCtrlId.compareAndSet(false, true)) {
            ctrlTaskContextId = contextId;
          }

        } else {
          LOG.log(Level.INFO, "Submitting GroupCommContext for ComputeNode to underlying context");

        }

        activeContext.submitContextAndService(contextConf, serviceConf);
      }
    }

    private String contextId(final Configuration contextConf) {
      try {
        final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
        return injector.getNamedInstance(ContextIdentifier.class);
      } catch (final InjectionException e) {
        throw new RuntimeException("Unable to inject context identifier from context conf", e);
      }
    }

    private boolean masterTaskSubmitted() {
      return !masterTaskSubmitted.compareAndSet(false, true);
    }
  }

  private String getSlaveId() { return "CmpTask-" + taskId.getAndIncrement(); }

  public class ContextCloseHandler implements EventHandler<ClosedContext> {

    @Override
    public void onNext(final ClosedContext closedContext) {
      LOG.log(Level.INFO, "Got closed context: {0}", closedContext.getId());
      final ActiveContext parentContext = closedContext.getParentContext();
      if (parentContext != null) {
        LOG.log(Level.INFO, "Closing parent context: {0}", parentContext.getId());
        parentContext.close();
      }
    }
  }

  public class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask completedTask) {

      final String taskId = completedTask.getId();
      LOG.log(Level.INFO, "Completed Task: {0}", taskId);

      final byte[] retBytes = completedTask.get();
      final String retStr = retBytes == null ? "No RetVal" : new String(retBytes);
      LOG.log(Level.INFO, "Result from {0} : {1}",
          new Object[]{taskId, retStr});

      LOG.log(Level.INFO, "Releasing Context: {0}", taskId);
      completedTask.getActiveContext().close();

    }

  }
}
