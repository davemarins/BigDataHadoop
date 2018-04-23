package exercise4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBigData extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String dates = "";
        for (Text value : values) {
            if (dates.isEmpty()) {
                dates = value.toString();
            } else {
                dates = dates.concat(value.toString());
            }
        }
        context.write(new Text(key), new Text(dates));

    }

}
