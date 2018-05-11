package lab3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

        String[] row = values.toString().split(",");

        for (String v1 : row) {
            for (String v2 : row) {
                if (v1.compareTo(v2) < 0) {
                    context.write(new Text(v1 + "," + v2), new IntWritable(1));
                }
            }
        }

    }
}
