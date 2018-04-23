package exercise10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    protected void map(LongWritable key, Text value, Context context) {

        context.getCounter(DriverBigData.MY_COUNTERS.TOTAL_RECORDS).increment(1);

    }

}
