package exercise28;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ReducerBigData extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        ArrayList<String> answers = new ArrayList<>();
        String question = "";

        for (Text value : values) {
            String row = value.toString();
            if (row.startsWith("Q")) {
                // It comes from Mapper1BigData Class
                question = row;
            } else {
                // It comes from Mapper2BigData Class
                answers.add(row);
            }
        }

        for (String answer : answers) {
            context.write(NullWritable.get(), new Text(question + "," + answer));
        }

    }

}
