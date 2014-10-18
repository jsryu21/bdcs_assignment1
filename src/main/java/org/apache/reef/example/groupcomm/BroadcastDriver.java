package org.apache.reef.example.groupcomm;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.io.network.nggroup.api.driver.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.driver.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.poison.PoisonedConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;
import org.apache.reef.example.data.loading.LineCountTask;
import org.apache.reef.example.groupcomm.operatorNames.ControlMessageBroadcaster;
import org.apache.reef.example.groupcomm.operatorNames.ModelBroadcaster;
import org.apache.reef.example.groupcomm.operatorNames.ModelReceiveAckReducer;
import org.apache.reef.example.groupcomm.parameters.AllCommunicationGroup;
import org.apache.reef.example.groupcomm.parameters.ModelDimensions;
import org.apache.reef.example.groupcomm.parameters.NumberOfReceivers;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver for Alternating Least Squares ML.
 */
@Unit
public final class BroadcastDriver {

  private static final Logger LOG = Logger.getLogger(BroadcastDriver.class.getName());

  private final AtomicBoolean masterSubmitted = new AtomicBoolean();
  private final AtomicInteger slaveIds = new AtomicInteger(0);
  private final AtomicInteger failureSet = new AtomicInteger(0);

  private final GroupCommDriver groupCommDriver;
  private final CommunicationGroupDriver allCommGroup;
  private final ConfigurationSerializer confSerializer;
  private final int dimensions;
  private final EvaluatorRequestor requestor;
  private final int numberOfReceivers;
  private final AtomicInteger numberOfAllocatedEvaluators;

  private String groupCommConfiguredMasterId;

  /**
   * Job driver constructor - instantiated via TANG.
   */
  @Inject
  public BroadcastDriver(
      final EvaluatorRequestor requestor,
      final GroupCommDriver groupCommDriver,
      final ConfigurationSerializer confSerializer,
      final @Parameter(ModelDimensions.class) int dimensions,
      final @Parameter(NumberOfReceivers.class) int numberOfReceivers) {

    this.requestor = requestor;
    this.groupCommDriver = groupCommDriver;
    this.confSerializer = confSerializer;
    this.dimensions = dimensions;
    this.numberOfReceivers = numberOfReceivers;
    this.numberOfAllocatedEvaluators = new AtomicInteger(numberOfReceivers + 1);

    this.allCommGroup = this.groupCommDriver.newCommunicationGroup(
        AllCommunicationGroup.class, numberOfReceivers + 1);

    LOG.log(Level.INFO, "Obtained all communication group");

    this.allCommGroup
        .addBroadcast(ControlMessageBroadcaster.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addBroadcast(ModelBroadcaster.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build()
        )
        .addReduce(ModelReceiveAckReducer.class,
            ReduceOperatorSpec.newBuilder()
              .setReceiverId(MasterTask.TASK_ID)
              .setDataCodecClass(SerializableCodec.class)
              .setReduceFunctionClass(ModelReceiveAckReduceFunction.class)
              .build())
        .finalise();

    LOG.log(Level.INFO, "Added operators to allCommGroup");
  }

  /**
   * Handles the StartTime event: Request numOfReceivers Evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final int numEvals = BroadcastDriver.this.numberOfReceivers + 1;
      LOG.log(Level.FINE, "Requesting {0} evaluators", numEvals);
      BroadcastDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(numEvals)
          .setMemory(2048)
          .build());
    }
  }

  /**
   * Handles AllocatedEvaluator: Submits a context with an id
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting an id context to AllocatedEvaluator: {0}", allocatedEvaluator);
      try {
        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "BroadcastContext-" +
                BroadcastDriver.this.numberOfAllocatedEvaluators.getAndDecrement())
            .build();
        allocatedEvaluator.submitContext(contextConfiguration);
      } catch (final BindException ex) {
        throw new RuntimeException("Unable to setup Task or Context configuration.", ex);
      }
    }
  }

  public class FailedTaskHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {

      LOG.log(Level.FINE, "Got failed Task: {0}", failedTask.getId());

      final ActiveContext activeContext = failedTask.getActiveContext().get();
      final Configuration partialTaskConf = Tang.Factory.getTang()
          .newConfigurationBuilder(
              TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, failedTask.getId())
                  .set(TaskConfiguration.TASK, SlaveTask.class)
                  .build(),
              PoisonedConfiguration.TASK_CONF
                  .set(PoisonedConfiguration.CRASH_PROBABILITY, "0")
                  .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
                  .build()
          )
          .bindNamedParameter(ModelDimensions.class, new Integer(dimensions).toString())
          .build();

      // Do not add the task back:
      // allCommGroup.addTask(partialTaskConf);

      final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
      LOG.log(Level.FINER, "Submit SlaveTask conf: {0}", confSerializer.toString(taskConf));

      activeContext.submitTask(taskConf);
    }
  }

  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    private final AtomicBoolean storeMasterId = new AtomicBoolean(false);

    @Override
    public void onNext(final ActiveContext activeContext) {

      LOG.log(Level.FINE, "Got active context: {0}", activeContext.getId());

      /**
       * The active context can be either from data loading service or after network
       * service has loaded contexts. So check if the GroupCommDriver knows if it was
       * configured by one of the communication groups.
       */
      if (groupCommDriver.isConfigured(activeContext)) {

        if (activeContext.getId().equals(groupCommConfiguredMasterId) && !masterTaskSubmitted()) {

          final Configuration partialTaskConf = Tang.Factory.getTang()
              .newConfigurationBuilder(
                  TaskConfiguration.CONF
                      .set(TaskConfiguration.IDENTIFIER, MasterTask.TASK_ID)
                      .set(TaskConfiguration.TASK, MasterTask.class)
                      .build()
              )
              .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
              .build();

          allCommGroup.addTask(partialTaskConf);

          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.log(Level.FINER, "Submit MasterTask conf: {0}", confSerializer.toString(taskConf));

          activeContext.submitTask(taskConf);

        } else {

          final Configuration partialTaskConf = Tang.Factory.getTang()
              .newConfigurationBuilder(
                  TaskConfiguration.CONF
                      .set(TaskConfiguration.IDENTIFIER, getSlaveId(activeContext))
                      .set(TaskConfiguration.TASK, SlaveTask.class)
                      .build(),
                  PoisonedConfiguration.TASK_CONF
                      .set(PoisonedConfiguration.CRASH_PROBABILITY, "0.4")
                      .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
                      .build())
              .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
              .build();

          allCommGroup.addTask(partialTaskConf);
          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.log(Level.FINER, "Submit SlaveTask conf: {0}", confSerializer.toString(taskConf));

          activeContext.submitTask(taskConf);
        }
      } else {

        final Configuration contextConf = groupCommDriver.getContextConfiguration();
        final String contextId = contextId(contextConf);

        if (storeMasterId.compareAndSet(false, true)) {
          groupCommConfiguredMasterId = contextId;
        }

        final Configuration serviceConf = groupCommDriver.getServiceConfiguration();
        LOG.log(Level.FINER, "Submit GCContext conf: {0}", confSerializer.toString(contextConf));
        LOG.log(Level.FINER, "Submit Service conf: {0}", confSerializer.toString(serviceConf));

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

    private String getSlaveId(final ActiveContext activeContext) { return "SlaveTask-" + slaveIds.getAndIncrement(); }

    private boolean masterTaskSubmitted() {
      return !masterSubmitted.compareAndSet(false, true);
    }
  }

  public class ContextCloseHandler implements EventHandler<ClosedContext> {

    @Override
    public void onNext(final ClosedContext closedContext) {
      LOG.log(Level.FINE, "Got closed context: {0}", closedContext.getId());
      final ActiveContext parentContext = closedContext.getParentContext();
      if (parentContext != null) {
        LOG.log(Level.FINE, "Closing parent context: {0}", parentContext.getId());
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
          new Object[] { taskId, retStr });

      LOG.log(Level.INFO, "Releasing Context: {0}", taskId);
      completedTask.getActiveContext().close();
    }
  }
}
