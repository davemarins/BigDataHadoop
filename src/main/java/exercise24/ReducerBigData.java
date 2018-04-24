package exercise24;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ReducerBigData extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String listOfFriends = "";
        for (Text value : values) {
            if (listOfFriends.isEmpty()) {
                listOfFriends = listOfFriends.concat(value.toString());
            } else {
                listOfFriends = listOfFriends.concat(", " + value.toString());
            }
        }
        context.write(new Text(key.toString() + ": "), new Text(listOfFriends));

    }

}
