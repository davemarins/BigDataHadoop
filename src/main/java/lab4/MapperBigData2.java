package lab4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperBigData2 extends Mapper<Text, Text, Text, DoubleWritable> {

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, new DoubleWritable(Double.parseDouble(value.toString())));
    }

}
