package exercise23ToDo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

class ReducerBigData extends Reducer<Text, Text, Text, NullWritable> {

    private HashSet<String> potentialFriends = new HashSet<>();
    private String specifiedUser;

    protected void setup(Context context) {
        specifiedUser = context.getConfiguration().get("username");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        boolean containsSpecifiedUser = false;
        HashSet<String> tempFriends = new HashSet<>();

        for (Text value : values) {
            if (specifiedUser.compareTo(value.toString()) == 0)
                containsSpecifiedUser = true;
            else {
                tempFriends.add(value.toString());
            }
        }

        if (containsSpecifiedUser && tempFriends.size() > 0) {
            for (String user : tempFriends) {
                potentialFriends.add(user);
            }
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        String result = "";
        for (String potFriend : potentialFriends) {
            result = result.concat(potFriend + " ");
        }
        context.write(new Text(result), NullWritable.get());

    }

}
