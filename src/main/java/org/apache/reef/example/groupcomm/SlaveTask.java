package org.apache.reef.example.groupcomm;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.task.Task;
import org.apache.reef.example.groupcomm.math.Vector;
import org.apache.reef.example.groupcomm.operatorNames.ControlMessageBroadcaster;
import org.apache.reef.example.groupcomm.operatorNames.ModelBroadcaster;
import org.apache.reef.example.groupcomm.operatorNames.ModelReceiveAckReducer;
import org.apache.reef.example.groupcomm.parameters.AllCommunicationGroup;

import javax.inject.Inject;

/**
 * Created by xyzi on 10/19/14.
 */
public class SlaveTask implements Task {

  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Receiver<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Receiver<Vector> modelBroadcaster;
  private final Reduce.Sender<Boolean> modelReceiveAckReducer;

  @Inject
  public SlaveTask(
      final GroupCommClient groupCommClient) {
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastReceiver(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastReceiver(ModelBroadcaster.class);
    this.modelReceiveAckReducer = communicationGroupClient.getReduceSender(ModelReceiveAckReducer.class);
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    boolean stop = false;
    while (!stop) {
      final ControlMessages controlMessage = controlMessageBroadcaster.receive();
      switch (controlMessage) {
        case Stop:
          stop = true;
          break;

        case ReceiveModel:
          modelBroadcaster.receive();
          if (Math.random() < 0.1) {
            throw new RuntimeException("Simulated Failure");
          }
          modelReceiveAckReducer.send(true);
          break;

        default:
          break;
      }
    }
    return null;
  }
}
