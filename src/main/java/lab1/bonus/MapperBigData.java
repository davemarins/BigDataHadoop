package lab1.bonus;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData extends Mapper<LongWritable, Text, Text, IntWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s+");
        for (int i = 0; i < words.length - 1; i++) {
            String row = words[i].toLowerCase() + " " + words[i + 1].toLowerCase();
            if (row.matches("[a-z0-9]+ [a-z0-9]+"))
                context.write(new Text(row), new IntWritable(1));
        }
    }
}
