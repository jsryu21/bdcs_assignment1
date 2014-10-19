package org.apache.reef.ml.als;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;
import org.apache.reef.ml.groupcomm.operatorNames.sub.ControlMessages;
import org.apache.reef.ml.math.DenseVector;
import org.apache.reef.ml.math.Vector;
import org.apache.reef.ml.groupcomm.AllCommGroup;
import org.apache.reef.ml.groupcomm.operatorNames.ControlMessageBroadcaster;
import org.apache.reef.ml.groupcomm.operatorNames.ModelBroadcaster;
import org.apache.reef.ml.groupcomm.operatorNames.ModelReducer;
import org.apache.reef.ml.parameters.Dimension;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ControllerTask implements Task {

  public static final String TASK_ID = "CtrlTask";

  private static final Logger LOG = Logger.getLogger(ControllerTask.class.getName());

  private final CommunicationGroupClient allCommGroup;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Sender<Vector> modelBroadcaster;
  private final Reduce.Receiver<ArrayList> modelReducer;

  private final int dimensions; /* dimension of  */

  private ArrayList<DenseVector> vectorsU = null;
  private ArrayList<DenseVector> vectorsM = null;
  private final int nu = 0; /* size of U */
  private final int nm = 0; /* size of M*/

  @Inject
  public ControllerTask(
      final GroupCommClient groupCommClient,
      final @Parameter(Dimension.class) int dimensions) {
    this.dimensions = dimensions;

    this.allCommGroup = groupCommClient.getCommunicationGroup(AllCommGroup.class);
    this.controlMessageBroadcaster = allCommGroup.getBroadcastSender(ControlMessageBroadcaster.class);
    this.modelBroadcaster = allCommGroup.getBroadcastSender(ModelBroadcaster.class);
    this.modelReducer = allCommGroup.getReduceReceiver(ModelReducer.class);
  }

  private void initVectorsU() {
    /*

     */
    vectorsU = new ArrayList<DenseVector>();

    for (int i = 0 ; i < nu ; i++) {
      new DenseVector(dimensions);
    }
  }


  private double calcRMSE() {
    double RMSE = 0;

    /*
    calculate RMSE with vectorU and vectorsM on Probeset.
     */

    return RMSE;
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {

    final Vector model = new DenseVector(dimensions);
    final long startTime = System.currentTimeMillis();
    double RMSE = 0;

    /*
    Distinguish nodes into movie and user.
     */
    controlMessageBroadcaster.send(ControlMessages.IdentifyMU);

    do {

      initVectorsU();

      controlMessageBroadcaster.send(ControlMessages.ComputeU);
      modelBroadcaster.send(model);
      vectorsM = modelReducer.reduce();

      controlMessageBroadcaster.send(ControlMessages.ComputeM);
      modelBroadcaster.send(model);
      vectorsU = modelReducer.reduce();

      final GroupChanges changes = allCommGroup.getTopologyChanges();
      if (changes.exist()) {
        LOG.log(Level.INFO, "There exist topology changes. Asking to update Topology");
        allCommGroup.updateTopology();
      } else {
        LOG.log(Level.INFO, "No changes in topology exist. So not updating topology");
      }

      RMSE = calcRMSE();

    } while (RMSE > 0.0001);

    final long finishTime = System.currentTimeMillis();
    LOG.log(Level.INFO, "Broadcasting vector of dimensions {0} took {1} secs",
        new Object[] { dimensions, (finishTime - startTime) / 1000.0 });

    controlMessageBroadcaster.send(ControlMessages.Stop);

    return null;
  }
}
