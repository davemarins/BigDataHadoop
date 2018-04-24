package exercise25;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class ReducerBigDataFilter extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String listOfPotentialFriends = "";
        HashSet<String> potentialFriends = new HashSet<>();
        for (Text value : values) {
            potentialFriends.add(value.toString());
        }
        for (String user : potentialFriends) {
            listOfPotentialFriends = listOfPotentialFriends.concat(user + " ");
        }
        context.write(key, new Text(listOfPotentialFriends));

    }

}
