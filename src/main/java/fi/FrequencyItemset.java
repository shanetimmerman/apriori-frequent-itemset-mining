package fi;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ListObjectsRequest;


public class FrequencyItemset extends Configured implements Tool {
	private final int MAX_ITERATION = Integer.MAX_VALUE;
	private final int MAX_MOVIE_ID = Integer.MAX_VALUE;
	private final int MAX_USER_ID = Integer.MAX_VALUE;
	private final double SUPPORT_CUTTOFF = 0.1;
	private final String INTERMEDIATE_PATH = "iteration";
	private final String BUCKET_NAME_STRING = "timmermansh-fi-mr";
	private final String BUCKET_NAME = "s3://" + BUCKET_NAME_STRING;
	private final boolean IS_LOCAL = true;

	private static final Logger logger = LogManager.getLogger(FrequencyItemset.class);

	@Override
	public int run(final String[] args) throws Exception {

		final Configuration conf = getConf();
		
		final Job job1 = Job.getInstance(conf, "Movie Set Initializer");
		job1.setJarByClass(FrequencyItemset.class);

		Configuration job1Conf = job1.getConfiguration();
		job1Conf.set("textinputformat.record.delimiter","\n\n");
		job1Conf.setInt("maxUserId", MAX_USER_ID);
		job1Conf.setInt("maxMovieId", MAX_MOVIE_ID);

		job1.setMapperClass(InitializeMapper.class);
		job1.setReducerClass(InitializeReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job1, args[0]);
		if (IS_LOCAL) {
			FileOutputFormat.setOutputPath(job1, new Path("iteration_0"));
		} else {
			FileOutputFormat.setOutputPath(job1, new Path(BUCKET_NAME + "/iteration_0"));
		}
		
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		int userCount = (int) job1.getCounters().findCounter(FrequencyItemsetCounters.USERS).getValue();

		final Job job2 = Job.getInstance(conf, "Movie Set Initializer");
		job2.setJarByClass(FrequencyItemset.class);

		Configuration job2Conf = job2.getConfiguration();
		job2Conf.set("textinputformat.record.delimiter","\n\n");
		job2Conf.setDouble("supportCutoff", SUPPORT_CUTTOFF);
		job2Conf.setInt("userCount", userCount);
		job2Conf.setInt("iterationNumber", 1);
		job2Conf.setBoolean("isLocal", IS_LOCAL);
		job2Conf.set("bucketName", BUCKET_NAME);
		job2Conf.setInt("maxUserId", MAX_USER_ID);
		job2Conf.setInt("maxMovieId", MAX_MOVIE_ID);

		job2.setMapperClass(IntermediateMapper.class);
		job2.setReducerClass(IntermediateReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job2, args[0]);
		if (IS_LOCAL) {
			FileOutputFormat.setOutputPath(job2, new Path("iteration_1"));
		} else {
			FileOutputFormat.setOutputPath(job2, new Path(BUCKET_NAME + "/iteration_1"));
		}

		MultipleOutputs.addNamedOutput(job2, "movies", TextOutputFormat.class, LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job2, "users", TextOutputFormat.class, LongWritable.class, Text.class);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		
		Counter itemSetCounter = job2.getCounters().findCounter(FrequencyItemsetCounters.ITEMSETS);

		for (int iteration = 2; iteration <= MAX_ITERATION; iteration++) {
			if (itemSetCounter.getValue() <= 1) {
				break;
			}
			
			Job job3 = Job.getInstance(conf, "Frequency Itemset Join Iteration " + iteration);

			final Configuration job3Conf = job3.getConfiguration();
			job3Conf.setInt("iterationNumber", iteration);
			job3Conf.setBoolean("isLocal", IS_LOCAL);
			job3Conf.set("bucketName", BUCKET_NAME);

			job3.setJarByClass(FrequencyItemset.class);

			job3.setInputFormatClass(TextInputFormat.class);
			job3.setOutputFormatClass(TextOutputFormat.class);

			if (IS_LOCAL) {
				TextInputFormat.addInputPaths(job3, "iteration_" + (iteration - 1) + "/itemsets");
				FileOutputFormat.setOutputPath(job3, new Path("join_iteration_" + iteration));	
			} else {
				TextInputFormat.addInputPaths(job3, BUCKET_NAME + "/iteration_" + (iteration - 1) + "/itemsets");
				FileOutputFormat.setOutputPath(job3, new Path(BUCKET_NAME + "/join_iteration_" + iteration));	
			}

			String fileDir = INTERMEDIATE_PATH + "_" + (iteration -1) + "/itemsets";
				
			if (IS_LOCAL) {
				for (String path : new File(fileDir).list()) {
					if (!path.endsWith(".crc") && !path.startsWith("_SUCCESS")) {
						job3.addCacheFile(new URI(fileDir + "/" + path));
					}
				}
			} else {
				final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
				
				ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(BUCKET_NAME_STRING)
				.withPrefix(fileDir);
				ObjectListing objects = s3Client.listObjects(listObjectsRequest);
				for (S3ObjectSummary summary: objects.getObjectSummaries()) {
					String path = summary.getKey();
					if (!path.endsWith(".crc") && !path.endsWith("_SUCCESS")) {
						logger.info("ADDED FILE " + BUCKET_NAME + "/" + path);
						job3.addCacheFile(new URI(BUCKET_NAME + "/" + path));
					}
				}
			}
			
			job3.setMapperClass(FrequencyItemsetJoinMapper.class);
			job3.setReducerClass(FrequencyItemsetJoinReducer.class);

			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(NullWritable.class);

			job3.getCacheFiles();

			if (!job3.waitForCompletion(true)) {
				System.exit(1);
			}
			
			Job job4 = Job.getInstance(conf, "Frequency Itemset Iteration " + iteration);

			final Configuration job4Conf = job4.getConfiguration();
			job4Conf.setInt("iterationNumber", iteration);
			job4Conf.setDouble("supportCutoff", SUPPORT_CUTTOFF);
			job4Conf.setInt("userCount", userCount);
			job4Conf.setBoolean("isLocal", IS_LOCAL);
			job4Conf.set("bucketName", BUCKET_NAME);

			job4.setJarByClass(FrequencyItemset.class);

			job4.setInputFormatClass(TextInputFormat.class);
			job4.setOutputFormatClass(TextOutputFormat.class);

			String userFolder = Integer.toString(iteration - 1) + "/users";

			if (IS_LOCAL) {
				TextInputFormat.addInputPaths(job4, "iteration_" + userFolder );
				FileOutputFormat.setOutputPath(job4, new Path("iteration_" + iteration));	
			} else {
				TextInputFormat.addInputPaths(job4, BUCKET_NAME + "/iteration_" + userFolder);
				FileOutputFormat.setOutputPath(job4, new Path(BUCKET_NAME + "/iteration_" + iteration));	
			}

			fileDir = "join_" + INTERMEDIATE_PATH + "_" + iteration;
				
			if (IS_LOCAL) {
				for (String path : new File(fileDir).list()) {
					if (!path.endsWith(".crc") && !path.startsWith("_SUCCESS")) {
						job4.addCacheFile(new URI(fileDir + "/" + path));
					}
				}
			} else {
				final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
				
				ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(BUCKET_NAME_STRING)
				.withPrefix(fileDir);
				ObjectListing objects = s3Client.listObjects(listObjectsRequest);
				for (S3ObjectSummary summary: objects.getObjectSummaries()) {
					String path = summary.getKey();
					if (!path.endsWith(".crc") && !path.endsWith("_SUCCESS")) {
						logger.info("ADDED FILE " + BUCKET_NAME + "/" + path);
						job4.addCacheFile(new URI(BUCKET_NAME + "/" + path));
					}
				}
			}

			fileDir = INTERMEDIATE_PATH + "_" + (iteration - 1) + "/itemsets";

			if (IS_LOCAL) {
				for (String path : new File(fileDir).list()) {
					if (!path.endsWith(".crc") && !path.startsWith("_SUCCESS")) {
						job4.addCacheFile(new URI(fileDir + "/" + path));
					}
				}
			} else {
				final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
				
				ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(BUCKET_NAME_STRING)
				.withPrefix(fileDir);
				ObjectListing objects = s3Client.listObjects(listObjectsRequest);
				for (S3ObjectSummary summary: objects.getObjectSummaries()) {
					String path = summary.getKey();
					if (!path.endsWith(".crc") && !path.endsWith("_SUCCESS")) {
						logger.info("ADDED FILE " + BUCKET_NAME + "/" + path);
						job4.addCacheFile(new URI(BUCKET_NAME + "/" + path));
					}
				}
			}
			
			MultipleOutputs.addNamedOutput(job4, "frequencyItemsets", TextOutputFormat.class, LongWritable.class, Text.class);
			MultipleOutputs.addNamedOutput(job4, "itemsetsForNextIteration", TextOutputFormat.class, LongWritable.class, Text.class);
			MultipleOutputs.addNamedOutput(job4, "usersForNextIteration", TextOutputFormat.class, LongWritable.class, Text.class);

			job4.setMapperClass(FrequencyItemsetMapper.class);
			job4.setReducerClass(FrequencyItemsetReducer.class);

			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(DoubleWritable.class);

			job4.getCacheFiles();

			if (!job4.waitForCompletion(true)) {
				System.exit(1);
			}
			itemSetCounter = job4.getCounters().findCounter(FrequencyItemsetCounters.ITEMSETS);
		}
		return 0;

	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new FrequencyItemset(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}