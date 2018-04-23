package exercise19;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData extends Mapper<Text, Text, NullWritable, Text> {

    private float threshold;

    protected void setup(Context context) {
        this.threshold = Float.parseFloat(context.getConfiguration().get("maxThreshold"));
    }

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = key.toString().split(",");
        float temperature = Float.parseFloat(fields[3]);
        if (temperature <= this.threshold) {
            context.write(NullWritable.get(), new Text(key.toString()));
        }
    }

}
