package fi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * We get the first item list and their support number, but what we really need is the frequency of
 * each item list. So for this mapper, we just calculate the frequency number by the total user count
 * and support.
 */
public class IntermediateMapper extends Mapper<LongWritable, Text, Text, Text> {
    private double freqCutOff;
    private int userCount;
    private int itemSetCount;
    private int maxUserId;
    private int maxMovieId;
    private final Text movie = new Text();
    private MultipleOutputs<Text,Text> mos;
    private int iterationNumber;
    private String prefix;
  
    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
        Configuration conf = context.getConfiguration();
    
        maxUserId = conf.getInt("maxUserId", -1);
        maxMovieId = conf.getInt("maxMovieId", -1);
        freqCutOff = conf.getDouble("supportCutoff", -1);
        userCount = conf.getInt("userCount", -1);
        mos = new MultipleOutputs<>(context);
    
        iterationNumber = conf.getInt("iterationNumber", -1);

        if (conf.getBoolean("isLocal", true)) {
            prefix = "";
        } else {
            prefix = conf.get("bucketName") + "/iteration_" + iterationNumber + "/";
        }

    }

    @Override
    public void map(final LongWritable key, final Text value, final Context context)
            throws IOException, InterruptedException {
        String[] row = value.toString().split(":");
        int movieId = Integer.parseInt(row[0]);
        if (movieId <= maxMovieId) {
            movie.set(row[0]);
            String[] users = row[1].trim().split("\n");
            List<String> filteredUsers = new ArrayList<String>();
            for (String user: users) {
                String[] splitRecord = user.split(",");
                int userId = Integer.parseInt(splitRecord[0]);
                if (userId < maxUserId) {
                    filteredUsers.add(splitRecord[0]);
                }
            }
            double probability = (double) filteredUsers.size() / userCount;
            if (probability > freqCutOff) {
                for (String record : filteredUsers) {
                    context.write(new Text(record), movie);
                }
                mos.write("movies", movie, new DoubleWritable(probability), prefix + "itemsets/part");
                itemSetCount++;
            }
        }        
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter(FrequencyItemsetCounters.ITEMSETS).increment(itemSetCount);
        mos.close();
    }
}
