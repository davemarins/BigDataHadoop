package exercise5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBigData extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        float result = 0;
        for (FloatWritable value : values) {
            result += Float.parseFloat(value.toString());
            count++;
        }
        result = result / count;

        context.write(new Text(key), new FloatWritable(result));

    }

}
