package exercise3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerBigData extends Reducer<Text, IntWritable, Text, IntWritable> {

    /*
     *
     * The input is the output of the Mapper (sensorID, 1)
     * The output would be (sensorID, #daysAboveThreshold) of the (key, value) record
     *
     */

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int occurrences = 0;
        for (IntWritable value : values) {
            occurrences += +value.get();
        }
        context.write(new Text(key), new IntWritable(occurrences));

    }

}
