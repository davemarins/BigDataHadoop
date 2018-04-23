package exercise7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperBigData extends Mapper<Text, Text, Text, Text> {

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");
        for (String word : words) {
            word = word.toLowerCase();
            if (!word.equals("and") && !word.equals("or") && !word.equals("not")) {
                context.write(new Text(word), new Text(key));
            }
        }
    }

}