package exercise14;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData extends Mapper<LongWritable, Text, Text, NullWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");
        for (String word : words) {
            word = word.toLowerCase();
            context.write(new Text(word), NullWritable.get());
        }

    }

}
