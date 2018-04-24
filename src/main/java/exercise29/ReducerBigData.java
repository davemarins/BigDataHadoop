package exercise29;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBigData extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String userData = "";
        Integer counter = 0;

        for (Text value : values) {
            String row = value.toString();
            counter++;
            if (row.startsWith("User:")) {
                userData = row.replaceFirst("User:", "");
            }
        }

        if (counter == 3) {
            context.write(NullWritable.get(), new Text(userData));
        }

    }

}
