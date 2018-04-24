package exercise29;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1BigData extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

        String[] fields = values.toString().split(",");
        // [0] userID; [3] gender; [4] birthYear
        context.write(new Text(fields[0]), new Text("User:" + fields[3] + "," + fields[4]));

    }

}
