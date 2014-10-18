package org.apache.reef.ml.als;

import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.tang.annotations.Unit;
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

    }
  }


}
