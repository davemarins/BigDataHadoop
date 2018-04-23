package exercise11;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerBigData extends Reducer<Text, CountSum, Text, CountSum> {

    @Override
    protected void reduce(Text key, Iterable<CountSum> values, Context context) throws IOException, InterruptedException {

        int globalCount = 0;
        float globalSum = 0;
        for (CountSum value : values) {
            globalSum += value.getSum();
            globalCount += value.getCount();
        }

        CountSum cs = new CountSum();
        cs.setCount(globalCount);
        cs.setSum(globalSum);

        context.write(new Text(key), cs);

    }

}
