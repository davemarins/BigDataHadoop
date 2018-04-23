package exercise17;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class Mapper1BigData extends Mapper<Text, Text, Text, FloatWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String date = fields[1];
        float temperature = Float.parseFloat(fields[3]);
        context.write(new Text(date), new FloatWritable(temperature));
    }

}
