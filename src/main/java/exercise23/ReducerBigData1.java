package exercise23;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ReducerBigData1 extends Reducer<Text, Text, Text, NullWritable> {

    private String specifiedUser;
    private List<String> potentialFriends = new ArrayList<>();
    private String wantedString = "X:";

    protected void setup(Context context) {
        this.specifiedUser = context.getConfiguration().get("username");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (key.toString().equals(specifiedUser)) {
            boolean first = true;
            for (Text friend : values) {
                if (first) {
                    this.wantedString = this.wantedString.concat(friend.toString());
                    first = false;
                } else {
                    this.wantedString = this.wantedString.concat("," + friend.toString());
                }
            }
        } else {
            boolean containsWantedUser = false;
            List<String> currentFriends = new ArrayList<>();
            for (Text friend : values) {
                if (friend.toString().equals(this.specifiedUser)) {
                    containsWantedUser = true;
                }
                currentFriends.add(friend.toString());
            }
            if (containsWantedUser) {
                for (String friendToBeAdded : currentFriends) {
                    if (!this.potentialFriends.contains(friendToBeAdded) &&
                            !friendToBeAdded.equals(this.specifiedUser)) {
                        this.potentialFriends.add(friendToBeAdded);
                    }
                }
            }
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        boolean first = true;
        String result = "";
        for (String friend : this.potentialFriends) {
            if (first) {
                result = result.concat(friend);
                first = false;
            } else {
                result = result.concat("," + friend);
            }

        }
        context.write(new Text(this.wantedString), NullWritable.get());
        context.write(new Text(result), NullWritable.get());
    }

}
