package org.apache.reef.example.data.loading;

import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.util.Pair;
import com.microsoft.reef.task.Task;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 'ALS' Task.
 */
public class LineCountTask implements Task {

  private static final Logger LOG = Logger.getLogger(LineCountTask.class.getName());

  private final DataSet<LongWritable, Text> dataSet;

  @Inject
  LineCountTask(DataSet<LongWritable, Text> dataSet) {

    this.dataSet = dataSet;
  }

  @Override
  public final byte[] call(final byte[] memento) {

    LOG.log(Level.INFO, "Als task started");
    int numEx = 0;
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      LOG.log(Level.INFO, "Read line: {0}", keyValue);
      ++numEx;
    }

    return Integer.toString(numEx).getBytes();
  }
}
