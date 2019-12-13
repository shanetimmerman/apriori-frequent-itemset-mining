package fi;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * we combine the number of the new frequent item list and calculate frequency, as well as the
 * confidence of the associations if applicable. We save the previous frequency result for the
 * latter user. The confidence of the associations is already part of our final results.
 */
public class FrequencyItemsetJoinReducer
    extends Reducer<Text, NullWritable, Text, NullWritable> {

  @Override
  public void reduce(final Text key, final Iterable<NullWritable> values, final Context context)
      throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }
}