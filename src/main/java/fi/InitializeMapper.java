package fi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This is the initial mapper to pre-processing the data and transform it to the format we want. The
 * input data come in format as:
 * 1:
 * 1,2,2005-05-05
 * 2,4,2005-05-05
 * 3,5,2005-05-05
 * 4,1,2005-05-05
 *
 * movie_id:
 * user_id, rating, data
 * user_id2, rating, data
 * ...
 *
 * for each movie, we emit (user_id, movie_id) and (movie_id, local_count)
 */
public class InitializeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private final Text movie = new Text();
    private Integer maxUserId;
    private Integer maxMovieId;
       
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        maxUserId = conf.getInt("maxUserId", -1);
        maxMovieId = conf.getInt("maxMovieId", -1);
    }

    @Override
    public void map(final LongWritable index, final Text value, final Context context)
            throws IOException, InterruptedException {
        String[] row = value.toString().split(":");
        int movieId = Integer.parseInt(row[0]);
        if (movieId <= maxMovieId) {
            movie.set(row[0]);
            for (String record : row[1].trim().split("\n")) {
                String[] splitRecord = record.split(",");
                int userId = Integer.parseInt(splitRecord[0]);
                if (userId < maxUserId) {
                    context.write(new Text(splitRecord[0]), NullWritable.get());
                }
            }
        }
    }

}
