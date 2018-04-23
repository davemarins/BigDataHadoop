package exercise8;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        Double result = 0.0;
        for (DoubleWritable value : values) {
            result += Double.parseDouble(value.toString());
            count++;
        }
        context.write(new Text(key), new DoubleWritable(result / count));

    }

}
