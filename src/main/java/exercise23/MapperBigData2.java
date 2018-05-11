package exercise23;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperBigData2 extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (value.toString().startsWith("X")) {
            String[] splitting = value.toString().split(":");
            String[] friends = splitting[1].split(",");
            for (String friend : friends) {
                context.write(new Text("X"), new Text(friend + "-X"));
            }
        } else {
            String[] friends = value.toString().split(",");
            for (String friend : friends) {
                context.write(new Text("X"), new Text(friend + "-U"));
            }
        }

    }

}
