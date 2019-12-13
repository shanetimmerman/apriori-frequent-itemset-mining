package fi;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
public class OutputWritable implements Writable {
    private final ArrayWritable ratedMovies = new ArrayWritable(Text.class);
    private final IntWritable reviewCount = new IntWritable();
    private boolean isReviewCount;

    public OutputWritable() {
    }

    public OutputWritable(Text[] ratedMovies) {
        this.ratedMovies.set(ratedMovies);
        this.isReviewCount = false;
    }

    public OutputWritable(int reviewCount) {
        this.reviewCount.set(reviewCount);
        this.isReviewCount = true;
    }

    
    public void readFields(DataInput in) throws IOException {

        BooleanWritable rankIsTrue = new BooleanWritable();
        rankIsTrue.readFields(in);

        this.isReviewCount = rankIsTrue.get();

        ratedMovies.readFields(in);
        reviewCount.readFields(in);        
    }
 
    public void write(DataOutput out) throws IOException {
        if (this.isReviewCount) {
            reviewCount.write(out);
        } else {
            this.ratedMovies.write(out);
        }
    }

    @Override
    public String toString() {
        if (this.isReviewCount) {
            return this.reviewCount.toString();
        } else {
            return this.ratedMovies.toString();
        }
    }
}
 
