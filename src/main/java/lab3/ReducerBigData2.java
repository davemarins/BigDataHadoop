package lab3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ReducerBigData2 extends Reducer<NullWritable, WordCountWritable, Text, IntWritable> {

    private Integer k = 100;

    @Override
    protected void reduce(NullWritable key, Iterable<WordCountWritable> values, Context context) throws IOException, InterruptedException {

        TopKVector<WordCountWritable> total = new TopKVector<>(k);
        for (WordCountWritable value : values) {
            total.updateWithNewElement(value);
        }
        for (WordCountWritable value : total.getLocalK()) {
            context.write(new Text(value.getWord()), new IntWritable(value.getCount()));
        }

    }

}
