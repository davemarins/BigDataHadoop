package exercise20;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

class MapperBigData extends Mapper<Text, Text, NullWritable, Text> {

    private float threshold;
    private MultipleOutputs<NullWritable, Text> outs = null;

    protected void setup(Context context) {
        this.threshold = Float.parseFloat(context.getConfiguration().get("maxThreshold"));
        this.outs = new MultipleOutputs<>(context);
    }

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = key.toString().split(",");
        float temperature = Float.parseFloat(fields[3]);
        if (temperature <= this.threshold) {
            this.outs.write("normaltemp", NullWritable.get(), new Text(key.toString()));
        } else {
            this.outs.write("hightemp", NullWritable.get(), new Text(key.toString()));
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.outs.close();
    }

}
