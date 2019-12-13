package fi;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
 
public class UserMovieKeyWritable implements WritableComparable {
    private final IntWritable id = new IntWritable();
    private final BooleanWritable isMovie = new BooleanWritable();

    public UserMovieKeyWritable() {
        this.id.set(0);
        this.isMovie.set(false);
    }

    public UserMovieKeyWritable(int r, boolean isMovie) {
        this.id.set(r);
        this.isMovie.set(isMovie);
    }

    public void readFields(DataInput in) throws IOException {
        isMovie.readFields(in);
        id.readFields(in);    
    }
 
    public void write(DataOutput out) throws IOException {
        isMovie.write(out);
        id.write(out);
    }

    public int getId() {
        return this.id.get();
    }

    public boolean isMovie() {
        return this.isMovie.get();
    }


    @Override
    public String toString() {
        return this.id.toString();
    }

    @Override
    public int compareTo(Object o) {
        if (o.getClass() == this.getClass()) {
            UserMovieKeyWritable that = (UserMovieKeyWritable) o;
            if (this.isMovie() && !that.isMovie()) {
                return 1;
            } else if (!this.isMovie() && that.isMovie()){
                return -1;
            } else {
                return this.getId() - that.getId();
            }
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (o.getClass() == this.getClass()) {
            UserMovieKeyWritable that = (UserMovieKeyWritable) o;
            return that.isMovie() == this.isMovie() && that.getId() == this.getId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getId(), this.isMovie());
    }
}
 
