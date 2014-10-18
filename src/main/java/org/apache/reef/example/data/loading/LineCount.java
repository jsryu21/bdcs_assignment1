package org.apache.reef.example.data.loading;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver for Alternating Least Squares ML.
 */
@Unit
public final class LineCount {

  private static final Logger LOG = Logger.getLogger(LineCount.class.getName());

  private final AtomicInteger ctrlCtxId = new AtomicInteger();
  private final AtomicInteger lineCnt = new AtomicInteger();
  private final AtomicInteger ctrlTaskId = new AtomicInteger();
  private final AtomicInteger totalTask = new AtomicInteger();

  private final DataLoadingService dataLoadingService;

  /**
   * Job driver constructor - instantiated via TANG.
   * @param dataLoadingService
   */
  @Inject
  public LineCount(final DataLoadingService dataLoadingService) {
    this.dataLoadingService = dataLoadingService;
    this.totalTask.set(dataLoadingService.getNumberOfPartitions());
  }

  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {

      final String contextId = activeContext.getId();
      LOG.log(Level.INFO, "Context active: {0}", contextId);

      if (dataLoadingService.isDataLoadedContext(activeContext)) {

        final String alsContextId = "AlsCtx-" + ctrlCtxId.getAndIncrement();
        LOG.log(Level.INFO, "Submit Als context {0} to: {1}",
            new Object[] { alsContextId, contextId });

        activeContext.submitContext(Tang.Factory.getTang()
            .newConfigurationBuilder(ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, alsContextId).build())
            .build());

      } else if (activeContext.getId().startsWith("AlsCtx")) {

        final String alsTaskId = "LineCountTask-" + ctrlTaskId.getAndIncrement();
        LOG.log(Level.INFO, "Submit Als task {0} to: {1}",
            new Object[] { alsTaskId, contextId });

        activeContext.submitTask(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, alsTaskId)
            .set(TaskConfiguration.TASK, LineCountTask.class)
            .build());

      } else {
        LOG.log(Level.INFO, "Als Task {0} -- Closing", contextId);
        activeContext.close();
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
      LOG.log(Level.INFO, "Line count from {0} : {1}",
          new Object[] { taskId, retStr });

      lineCnt.addAndGet(Integer.parseInt(retStr));

      if (totalTask.decrementAndGet() <= 0) {
        LOG.log(Level.INFO, "Total line count: {0}", lineCnt.get());
      }

      LOG.log(Level.INFO, "Releasing Context: {0}", taskId);
      completedTask.getActiveContext().close();
    }
  }
}
