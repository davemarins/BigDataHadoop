package exercise29;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2BigData extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

        String[] fields = values.toString().split(",");
        // [0] userID; [1] movie genre
        if (fields[1].compareTo("Commedia") == 0 || fields[1].compareTo("Adventure") == 0) {
            context.write(new Text(fields[0]), new Text("Like"));
        }

    }

}
