package exercise23ToDo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

class ReducerBigDataFilter extends Reducer<NullWritable, Text, Text, NullWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashSet<String> potentialFriends = new HashSet<>();

        for (Text value : values) {
            String[] users = value.toString().split(" ");
            for (String user : users) {
                potentialFriends.add(user);
            }
        }

        String globalPotFriends = "";
        for (String potFriend : potentialFriends) {
            globalPotFriends = globalPotFriends.concat(potFriend + " ");
        }

        context.write(new Text(globalPotFriends), NullWritable.get());

    }

}
