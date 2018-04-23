package exercise1and2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBigData extends Reducer<Text, IntWritable, Text, IntWritable> {

    /*
     * The Reducer is composed of:
     *
     * 1) input key type
     * 2) input value type
     * 3) output key type
     * 4) output value type
     *
     * The input is the output of the Mapper (singleWord, 1)
     * The output would be (singleWord, #occurrences) of the (key, value) record
     *
     */

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int occurrences = 0;
        for (IntWritable value : values) {
            occurrences = occurrences + value.get();
        }
        context.write(key, new IntWritable(occurrences));

    }

}
