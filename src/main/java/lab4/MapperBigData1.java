package lab4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData1 extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields[0].compareTo("Id") != 0) {
            // (userID, productID:Score)
            context.write(new Text(fields[2]), new Text(fields[1] + ":" + fields[6]));
        }
    }

}
