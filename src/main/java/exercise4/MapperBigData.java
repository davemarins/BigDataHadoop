package exercise4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperBigData extends Mapper<Text, Text, Text, Text> {

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 50ug / m^3
        Double threshold = 50.0;
        String[] fields = key.toString().split(",");
        String sensorID = fields[0];
        String date = fields[1];
        Double sensorValue = Double.parseDouble(value.toString());
        if (sensorValue > threshold) {
            context.write(new Text(sensorID), new Text(date));
        }
    }

}
