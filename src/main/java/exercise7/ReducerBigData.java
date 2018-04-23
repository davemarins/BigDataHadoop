package exercise7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBigData extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String sentenceIds = "[";
        boolean first = true;
        for (Text text : values) {
            if (first) {
                sentenceIds = sentenceIds.concat(text.toString());
                first = false;
            } else {
                sentenceIds = sentenceIds.concat("," + text.toString());
            }
        }
        sentenceIds = sentenceIds.concat("])");
        String newKey = "(";
        newKey = newKey.concat(key.toString() + ",");
        context.write(new Text(newKey), new Text(sentenceIds));

    }

}
