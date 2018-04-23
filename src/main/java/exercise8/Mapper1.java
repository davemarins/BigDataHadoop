package exercise8;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<Text, Text, Text, DoubleWritable> {

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String[] date = key.toString().split("-");
        context.write(new Text(date[0] + "-" + date[1]), new DoubleWritable(Double.parseDouble(value.toString())));

    }

}
