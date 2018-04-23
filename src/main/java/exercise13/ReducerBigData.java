package exercise13;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ReducerBigData extends Reducer<NullWritable, Text, Text, FloatWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) {
        try {
            Income i1 = new Income();
            i1.setDate("2000-01-01");
            i1.setIncome(0);
            Income i2 = new Income();
            i2.setDate("2000-01-01");
            i2.setIncome(0);
            for (Text value : values) {
                String[] fields = value.toString().split("_");
                float dayIncome = Float.parseFloat(fields[1]);
                if (dayIncome > i1.getIncome()) {
                    i2.setIncome(i1.getIncome());
                    i2.setDate(i1.getDate());
                    i1.setIncome(dayIncome);
                    i1.setDate(fields[0]);
                } else if (dayIncome > i2.getIncome()) {
                    i2.setIncome(dayIncome);
                    i2.setDate(fields[0]);
                }
            }
            context.write(new Text(i1.getDate()), new FloatWritable(i1.getIncome()));
            context.write(new Text(i2.getDate()), new FloatWritable(i2.getIncome()));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
