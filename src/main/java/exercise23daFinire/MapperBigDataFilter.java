package exercise23daFinire;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Exercise 23 - Mapper
 */
class MapperBigDataFilter extends
        Mapper<LongWritable, // Input key type
                Text, // Input value type
                NullWritable, // Output key type
                Text> {// Output value type

    // Identity mapper
    protected void map(LongWritable key, // Input key type
                       Text value, // Input value type
                       Context context) throws IOException, InterruptedException {

        context.write(NullWritable.get(), new Text(value.toString()));
    }

}
