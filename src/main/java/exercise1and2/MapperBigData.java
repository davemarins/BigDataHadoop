package exercise1and2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperBigData extends Mapper<LongWritable, Text, Text, IntWritable> {

    /*
     *
     * The Mapper is composed of:
     *
     * 1) input key type
     * 2) input value type
     * 3) output key type
     * 4) output value type
     *
     * The input is a unstructured textual file, in this case the key type is not important
     * The input value is of course a Text (-> see toString().split(regex))
     * The output of the Mapper would be a (singleWord, 1) so a Text and a IntWritable
     *
     */

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");
        for (String word : words) {
            String cleanedWord = word.toLowerCase();
            context.write(new Text(cleanedWord), new IntWritable(1));
        }

    }

}
