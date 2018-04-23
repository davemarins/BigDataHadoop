package exercise15;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ReducerBigData extends Reducer<Text, NullWritable, Text, IntWritable> {

    private int wordID;

    protected void setup(Context context) {
        this.wordID = 0;
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        this.wordID++;
        context.write(key, new IntWritable(this.wordID));
    }

}
