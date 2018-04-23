package exercise9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class MapperBigData extends Mapper<LongWritable, Text, Text, IntWritable> {

    private HashMap<String, Integer> wordsCounts;

    protected void setup(Context context) {
        wordsCounts = new HashMap<>();
    }

    protected void map(LongWritable key, Text value, Context context) {

        Integer currentFrequency;
        String[] words = value.toString().split("\\s+");

        for (String word : words) {

            String cleanWord = word.toLowerCase();

            currentFrequency = this.wordsCounts.get(cleanWord);
            if (currentFrequency == null) {
                this.wordsCounts.put(cleanWord, 1);
            } else {
                currentFrequency++;
                this.wordsCounts.put(cleanWord, currentFrequency);
            }
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (Entry<String, Integer> pair : this.wordsCounts.entrySet()) {
            context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
        }

    }

}
