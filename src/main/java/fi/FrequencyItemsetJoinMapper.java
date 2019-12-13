package fi;

import com.google.common.collect.Sets;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class FrequencyItemsetJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
  private final List<Set<String>> previousItemsets = new ArrayList<>();
  private int iterationNumber;
  private static final Logger logger = LogManager.getLogger(FrequencyItemsetMapper.class);

  @Override
  public void setup(Context context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();

    iterationNumber = conf.getInt("iterationNumber", -1);

    FileSystem fileSystem;
    String uriPath;
    if (conf.getBoolean("isLocal", true)) {
      uriPath = ".";
    } else {
      uriPath = conf.get("bucketName");
    }

    try {
      fileSystem = FileSystem.get(new URI(uriPath), conf);
    } catch (URISyntaxException e) {
      throw new IOException(e.getMessage());
    }
    for (URI cacheFile : context.getCacheFiles()) {
      Path filePath = new Path(cacheFile);
      BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));

      String row = "";

      while ((row = reader.readLine()) != null) {
        if (!row.isEmpty()) {
          String[] splitRow = row.split("\t");
          String[] rowData = splitRow[0].split(",");
          previousItemsets.add(new HashSet<>(Arrays.asList(rowData)));
        }
      }
    }
  }


  @Override
  public void map(final LongWritable key, final Text row, final Context context)
      throws IOException, InterruptedException {
    // key is user_id which we don't care, row is the record movie list
    String[] splitRow = row.toString().split("\t");
    String[] rowData = splitRow[0].split(",");
    Set<String> setCopy = new HashSet<>(Arrays.asList(rowData));

    for (Set<String> previousSet : previousItemsets) {
      Set<String> union = Sets.union(setCopy, previousSet);
      if (union.size() == iterationNumber) {
        String[] unionArray = new String[union.size()];
        unionArray = union.toArray(unionArray);
        Arrays.sort(unionArray);
        
        context.write(new Text(String.join(",", unionArray)), NullWritable.get());
      }
    }
  }
}
