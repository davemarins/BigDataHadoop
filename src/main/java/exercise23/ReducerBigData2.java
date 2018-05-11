package exercise23;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerBigData2 extends Reducer<Text, Text, Text, NullWritable> {

    private List<String> friendsOfWantedUser = new ArrayList<>();
    private List<String> potentialResult = new ArrayList<>();
    private List<String> result = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text friend : values) {
            String[] fields = friend.toString().split("-");
            if (fields[1].equals("X")) {
                this.friendsOfWantedUser.add(fields[0]);
            } else {
                this.potentialResult.add(fields[0]);
            }
        }
        for (String friend : potentialResult) {
            if (!this.friendsOfWantedUser.contains(friend)) {
                this.result.add(friend);
            }
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        boolean first = true;
        String finalResult = "";
        for (String friend : this.result) {
            if (first) {
                finalResult = finalResult.concat(friend);
                first = false;
            } else {
                finalResult = finalResult.concat("," + friend);
            }

        }
        context.write(new Text(finalResult), NullWritable.get());
    }

}
