package exercise25;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

class ReducerBigData extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashSet<String> users = new HashSet<>();
        for (Text value : values) {
            users.add(value.toString());
        }
        for (String user : users) {
            String potentialFriends = "";
            for (String friend : users) {
                if (user.compareTo(friend) != 0) {
                    if (potentialFriends.isEmpty()) {
                        potentialFriends = potentialFriends.concat(friend);
                    } else {
                        potentialFriends = potentialFriends.concat("," + friend);
                    }
                }
            }
            if (!potentialFriends.isEmpty()) {
                context.write(new Text(user), new Text(potentialFriends));
            }
        }

    }

}
