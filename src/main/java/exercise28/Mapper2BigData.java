package exercise28;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2BigData extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

        String[] fields = values.toString().split(",");
        // [0] answerID; [1] questionID; [3] answerText
        context.write(new Text(fields[1]), new Text(fields[0] + "," + fields[3]));

    }

}
