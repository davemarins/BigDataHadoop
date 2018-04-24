package exercise23daFinire;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Exercise 23 - Reducer
 */
class ReducerBigDataFilter extends
        Reducer<NullWritable, // Input key type
                Text, // Input value type
                Text, // Output key type
                NullWritable> { // Output value type

    @Override
    protected void reduce(NullWritable key, // Input key type
                          Iterable<Text> values, // Input value type
                          Context context) throws IOException, InterruptedException {

        HashSet<String> potentialFriends = new HashSet<String>();

        for (Text value : values) {
            // Extract the list of users in the current line
            String[] users = value.toString().split(" ");

            for (String user : users) {
                // If the user is new then it is inserted in the set
                // Otherwise, it is already in the set, it is ignored
                potentialFriends.add(user);
            }
        }

        String globalPotFriends;

        // Concatenate the users in the set potentialFriends
        globalPotFriends = new String("");

        for (String potFriend : potentialFriends) {
            globalPotFriends = globalPotFriends.concat(potFriend + " ");
        }

        context.write(new Text(globalPotFriends), NullWritable.get());

    }
}
