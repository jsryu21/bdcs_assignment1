package org.apache.reef.ml.als;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;
import org.apache.reef.ml.groupcomm.operatorNames.sub.ControlMessages;
import org.apache.reef.ml.math.DenseVector;
import org.apache.reef.ml.math.Vector;
import org.apache.reef.ml.groupcomm.operatorNames.ControlMessageBroadcaster;
import org.apache.reef.ml.groupcomm.operatorNames.ModelBroadcaster;
import org.apache.reef.ml.groupcomm.AllCommGroup;
import org.apache.reef.ml.groupcomm.operatorNames.ModelReducer;
import org.apache.reef.ml.parameters.Lambda;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class ComputeTask implements Task {

  private static AtomicInteger taskNum = new AtomicInteger(0);
  private final double lambda;

  private final CommunicationGroupClient allCommGroup;
  private final Broadcast.Receiver<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Receiver<Vector> modelBroadcaster;
  private final Reduce.Sender<ArrayList> modelReducer;

  @Inject
  public ComputeTask(
      final GroupCommClient groupCommClient,
      final @Parameter(Lambda.class) double lambda) {
    this.lambda = lambda;

    this.allCommGroup = groupCommClient.getCommunicationGroup(AllCommGroup.class);
    this.controlMessageBroadcaster = allCommGroup.getBroadcastReceiver(ControlMessageBroadcaster.class);
    this.modelBroadcaster = allCommGroup.getBroadcastReceiver(ModelBroadcaster.class);
    this.modelReducer = allCommGroup.getReduceSender(ModelReducer.class);
  }

  private boolean isM() {
    /*
    Identify this task's split is U or M with linetag in split.
     */

    return true;
  }

  private ArrayList calcM() {
    ArrayList<DenseVector> vectorList = null;
    return vectorList;
  }

  private ArrayList calcU() {
    ArrayList<DenseVector> vectorList = null;
    return vectorList;
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {

    boolean stop = false;
    boolean M = false;
    boolean U = false;

    while (!stop) {
      final ControlMessages controlMessage = controlMessageBroadcaster.receive();
      switch (controlMessage) {
        case Stop:
          stop = true;
          break;

        case IdentifyMU:
          if (isM())
            M = true;
          else
            U = true;
          break;
        case ComputeM:
          if (M) {
            modelBroadcaster.receive();

            modelReducer.send(calcM());
          }
          break;
        case ComputeU:
          if (U) {
            modelBroadcaster.receive();

            modelReducer.send(calcU());
          }
          break;
        default:
          break;
      }
    }
    return null;
  }
}
