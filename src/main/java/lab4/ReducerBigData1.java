package lab4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReducerBigData1 extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashMap<String, Double> productsRating = new HashMap<>();
        Double totalRatings = 0.0, average;
        Integer numberRatings = 0;

        for (Text value : values) {
            // productID:score
            String[] fields = value.toString().split(":");
            productsRating.put(fields[0], Double.parseDouble(fields[1]));
            totalRatings += Double.parseDouble(fields[1]);
            numberRatings++;
        }
        average = totalRatings / numberRatings;

        for (Map.Entry<String, Double> pair : productsRating.entrySet()) {
            context.write(new Text(pair.getKey()),
                    new DoubleWritable(pair.getValue() - average));
        }

    }

}
