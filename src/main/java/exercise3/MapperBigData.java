package exercise3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class MapperBigData extends Mapper<Text, Text, Text, IntWritable> {

    private static Double threshold = 50.0; // 50ug per m3

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = key.toString().split(",");
        String sensorID = fields[0];
        Double sensorValue = Double.parseDouble(value.toString());
        if (sensorValue > threshold) {
            context.write(new Text(sensorID), new IntWritable(1));
        }

    }

}
