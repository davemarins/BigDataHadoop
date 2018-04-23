package exercise17;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ReducerBigData extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float maxValue = Float.MIN_VALUE;
        for (FloatWritable value : values) {
            float current = value.get();
            if (current > maxValue) {
                maxValue = current;
            }
        }
        context.write(key, new FloatWritable(maxValue));
    }

}
