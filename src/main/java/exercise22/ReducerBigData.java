package exercise22;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ReducerBigData extends Reducer<NullWritable, Text, Text, NullWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String friends = "";
        for (Text friend : values) {
            if (friends.isEmpty()) {
                friends = friends.concat(friend.toString());
            } else {
                friends = friends.concat("," + friend.toString());
            }
        }
        context.write(new Text(friends), NullWritable.get());

    }

}
