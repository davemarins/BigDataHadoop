package lab1.bonus;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<LongWritable, Text, Text, IntWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s+");
        for (int i = 0; i < words.length - 1; i++) {
            String biGram = words[i] + " " + words[i + 1];
            biGram = biGram.toLowerCase();
            if (biGram.matches("[a-z0-9]+ [a-z0-9]+"))
                context.write(new Text(biGram), new IntWritable(1));
        }
    }
}
