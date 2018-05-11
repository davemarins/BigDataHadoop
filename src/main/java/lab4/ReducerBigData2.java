package lab4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBigData2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Integer count = 0;
        Double totalRatings = 0.0;
        for (DoubleWritable value : values) {
            totalRatings += value.get();
            count++;
        }
        context.write(key, new DoubleWritable(totalRatings / count));
    }

}
