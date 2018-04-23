package exercise12;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData extends Mapper<Text, Text, Text, FloatWritable> {

    private float threshold;

    protected void setup(Context context) {
        this.threshold = Float.parseFloat(context.getConfiguration().get("maxThreshold"));
    }

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        float measure = Float.parseFloat(value.toString());
        if (measure < this.threshold) {
            context.write(key, new FloatWritable(measure));
        }
    }

}
