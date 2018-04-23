package exercise13;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData extends Mapper<Text, Text, NullWritable, Text> {

    private Income i1;
    private Income i2;

    protected void setup(Context context) {
        this.i1 = new Income();
        this.i1.setIncome(0);
        this.i1.setDate("2000-01-01");
        this.i2 = new Income();
        this.i2.setIncome(0);
        this.i2.setDate("2000-01-01");
    }

    protected void map(Text key, Text value, Context context) {
        String dateIncome = key.toString();
        float dayIncome = Float.parseFloat(value.toString());
        if (dayIncome > i1.getIncome()) {
            this.i2.setIncome(this.i1.getIncome());
            this.i2.setDate(this.i1.getDate());
            this.i1.setIncome(dayIncome);
            this.i1.setDate(dateIncome);
        } else if (dayIncome > i2.getIncome()) {
            this.i2.setIncome(dayIncome);
            this.i2.setDate(dateIncome);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), new Text(this.i1.toString()));
        context.write(NullWritable.get(), new Text(this.i2.toString()));
    }

}
