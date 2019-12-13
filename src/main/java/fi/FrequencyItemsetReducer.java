package fi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * we combine the number of the new frequent item list and calculate frequency, as well as the
 * confidence of the associations if applicable. We save the previous frequency result for the
 * latter user. The confidence of the associations is already part of our final results.
 */
public class FrequencyItemsetReducer
    extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  private double freqCutoff;
  private MultipleOutputs<Text, DoubleWritable> mos;
  private final DoubleWritable frequency = new DoubleWritable();
  private int userCount;
  private Map<Set<String>, Double> previousItemsetDictionary;
  private int itemSetCount;
  private int iterationNumber;
  private String prefix;

  @Override
  public void setup(Context context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();

    freqCutoff = conf.getDouble("supportCutoff", -1);
    userCount = conf.getInt("userCount", -1);
    mos = new MultipleOutputs<>(context);

    iterationNumber = conf.getInt("iterationNumber", -1);

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
    previousItemsetDictionary = new HashMap<>();

    for (URI cacheFile : context.getCacheFiles()) {
      if (!cacheFile.getPath().contains("join_")) {
        Path filePath = new Path(cacheFile.toString());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));

        String row = "";

        while ((row = reader.readLine()) != null) {
          if (!row.isEmpty()) {
            String[] splitRow = row.split("\t");
            double frequency = Double.parseDouble(splitRow[1]);
            String[] rowData = splitRow[0].split(",");
            previousItemsetDictionary.put(new HashSet<>(Arrays.asList(rowData)), frequency);
          }
        }
      }
    }
  }

  @Override
  public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context)
      throws IOException, InterruptedException {
    int count = 0;

    for (DoubleWritable value : values) {
      count += value.get();
    }

    double freuqencyValue = (double) count / userCount;

    if (freuqencyValue > freqCutoff) {
      frequency.set(freuqencyValue);
      mos.write("itemsetsForNextIteration", key, frequency, prefix + "itemsets/part");
      
      itemSetCount++;
      Set<String> currentItemset = new HashSet<String>(Arrays.asList(key.toString().split(",")));
      for (String item : currentItemset) {
        Set<String> difference = new HashSet<>(currentItemset);
        difference.remove(item);
        mos.write("frequencyItemsets",
          new Text(difference + " -> " + item),
          new DoubleWritable(freuqencyValue / previousItemsetDictionary.get(difference)), prefix + "probabilities/part");
      }
    }
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    context.getCounter(FrequencyItemsetCounters.ITEMSETS).increment(itemSetCount);
    mos.close();
  }
}