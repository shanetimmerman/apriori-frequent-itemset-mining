package fi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * This is the initial reducer. It collects all the movie id of one user. And collect all the counts
 * for a movie id. Also we are using a global counter to count the number of the users.
 */
public class IntermediateReducer
        extends Reducer<Text, Text, Text, Text> {                
    private MultipleOutputs<Text, Text> mos;
    private int iterationNumber;
    private String prefix;
          
    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
        Configuration conf = context.getConfiguration();
    
        mos = new MultipleOutputs<>(context);
    
        iterationNumber = conf.getInt("iterationNumber", -1);
        String uriPath;
    
        if (conf.getBoolean("isLocal", true)) {
            uriPath = ".";
            prefix = "";
        } else {
            uriPath = conf.get("bucketName");
            prefix = uriPath + "/iteration_" + iterationNumber + "/";
        }
    }

    @Override
    public void reduce(final Text user, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
        List<String> movies = new ArrayList<String>();
        for (Text val : values) {
            movies.add(val.toString());
        }
        mos.write("users", user, new Text(String.join(",", movies)), "users/part");
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
}