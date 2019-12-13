package fi;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the initial reducer. It collects all the movie id of one user. And collect all the counts
 * for a movie id. Also we are using a global counter to count the number of the users.
 */
public class InitializeReducer
        extends Reducer<Text, NullWritable, Text, NullWritable> {
    private Counter userCounter;
    private int userCount;
                
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        userCounter = context.getCounter(FrequencyItemsetCounters.USERS);
        userCount = 0;
    }

    @Override
    public void reduce(final Text key, final Iterable<NullWritable> values, final Context context)
            throws IOException, InterruptedException {
        userCount++;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        userCounter.increment(userCount);
    }
}