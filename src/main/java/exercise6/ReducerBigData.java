package exercise6;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBigData extends Reducer<Text, FloatWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        float max = -1, min = 999; // I mean, that's enough xD
        String result;
        for (FloatWritable value : values) {
            if (max < Float.parseFloat(value.toString())) {
                max = Float.parseFloat(value.toString());
            }
            if (min > Float.parseFloat(value.toString())) {
                min = Float.parseFloat(value.toString());
            }
        }
        result = "max=" + max + "_min=" + min;
        context.write(new Text(key), new Text(result));

    }

}
