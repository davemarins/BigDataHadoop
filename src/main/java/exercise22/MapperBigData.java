package exercise22;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperBigData extends Mapper<LongWritable, Text, NullWritable, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String givenUser = context.getConfiguration().get("username");
        String[] users = value.toString().split(",");
        if (users[0].equals(givenUser)) {
            context.write(NullWritable.get(), new Text(users[1]));
        } else if (users[1].equals(givenUser)) {
            context.write(NullWritable.get(), new Text(users[0]));
        }
    }

}
