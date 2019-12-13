package fi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * The mapper of Apriori algorithms. It loads the previous frequent item list from previous frequent
 * item list file cache and construct the new ones. We then calculate the support number of each new
 * item list and emit them.
 */
public class FrequencyItemsetMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  private final DoubleWritable one = new DoubleWritable(1);
  private Set<Set<String>> newItemsets;
  private MultipleOutputs<Text, DoubleWritable> mos;
  private String prefix;
  private static final Logger logger = LogManager.getLogger(FrequencyItemsetMapper.class);



  @Override
  public void setup(Context context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();

    int iterationNumber = conf.getInt("iterationNumber", -1);

    mos = new MultipleOutputs<>(context);

    FileSystem fileSystem;
    String uriPath;
    if (conf.getBoolean("isLocal", true)) {
      uriPath = ".";
      prefix = "";
    } else {
      uriPath = conf.get("bucketName");
      prefix = uriPath + "/iteration_" + iterationNumber + "/";
    }

    try {
      fileSystem = FileSystem.get(new URI(uriPath), conf);
    } catch (URISyntaxException e) {
      throw new IOException(e.getMessage());
    }

    newItemsets = new HashSet<>();

    for (URI cacheFile : context.getCacheFiles()) {
      if (cacheFile.getPath().contains("join_")) {
        Path filePath = new Path(cacheFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));

        String row = "";

        while ((row = reader.readLine()) != null) {
          if (!row.isEmpty()) {
            String[] splitRow = row.split(",");
            newItemsets.add(new HashSet<>(Arrays.asList(splitRow)));
          }
        }
      }
    }
  }


  @Override
  public void map(final LongWritable key, final Text row, final Context context)
      throws IOException, InterruptedException {
    // key is user_id which we don't care, row is the record movie list
    String[] splitRow = row.toString().split("\t");

    String moviesString = splitRow[1];

    Set<String> movies = new HashSet<>(Arrays.asList(moviesString.split(",")));

    boolean any = false;

    for (Set<String> itemSet : newItemsets) {
      if (movies.containsAll(itemSet)) {
        any = true;
        // if the new item set if the subset of the record, we emit one
        context.write(new Text(String.join(",", itemSet)), one);
      }
    }
    if (any) {
      mos.write("usersForNextIteration",
            new Text(splitRow[0]),
            new Text(splitRow[1]), prefix + "users/part");
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
      if (null != mos) {
          mos.close();
      }
  }
}
