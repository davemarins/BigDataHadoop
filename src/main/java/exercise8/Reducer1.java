package exercise8;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        Double total = 0.0;
        for (DoubleWritable value : values) {
            total += Double.parseDouble(value.toString());
        }
        context.write(new Text(key), new DoubleWritable(total));

    }

}
