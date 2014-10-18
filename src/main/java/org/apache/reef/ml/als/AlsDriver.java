package org.apache.reef.ml.als;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver for Alternating Least Squares ML.
 */
@Unit
public final class AlsDriver {

  private static final Logger LOG = Logger.getLogger(AlsDriver.class.getName());

  private EvaluatorRequestor requestor = null;

  /**
   * Job driver constructor - instantiated via TANG.
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public AlsDriver(final EvaluatorRequestor requestor) {
    LOG.log(Level.INFO, "Instantiated 'AlsDriver'");

    this.requestor = requestor;
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  public final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "Request Evaluator");

      AlsDriver.this.requestor.submit(
        EvaluatorRequest.newBuilder()
          .setMemory(128)
          .setNumber(3)
          .setNumberOfCores(1)
          .build()
      );
    }
  }

  /**
   * Handles AllocatedEvaluator: Build a Context & Task Configuration
   * and submit them to the Driver
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting ALS task to AllocatedEvaluator: {0}", allocatedEvaluator);
      try {
        final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "AlsContext").build();

        final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "AlsTask")
          .set(TaskConfiguration.TASK, AlsTask.class).build();

        // Let's submit context and task to the evaluator
        allocatedEvaluator.submitContextAndTask(contextConfiguration, taskConfiguration);
      } catch (final BindException ex) {
        throw new RuntimeException("Unable to setup Task or Context configuration.", ex);
      }
    }
  }

}
