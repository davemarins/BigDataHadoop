package exercise25;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] users = value.toString().split(",");
        context.write(new Text(users[0]), new Text(users[1]));
        context.write(new Text(users[1]), new Text(users[0]));

    }

}
