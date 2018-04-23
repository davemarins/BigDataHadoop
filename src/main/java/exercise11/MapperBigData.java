package exercise11;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class MapperBigData extends Mapper<LongWritable, Text, Text, CountSum> {

    private HashMap<String, CountSum> statistics;

    protected void setup(Context context) {
        this.statistics = new HashMap<>();
    }

    protected void map(LongWritable key, Text value, Context context) {

        CountSum cs;
        String[] fields = value.toString().split(",");
        String sensorID = fields[0];
        Float valueRead = Float.parseFloat(fields[2]);
        cs = this.statistics.get(sensorID);
        if (cs == null) {
            cs = new CountSum();
            cs.setCount(1);
            cs.setSum(valueRead);
            this.statistics.put(sensorID, cs);
        } else {
            cs.setSum(cs.getSum() + valueRead);
            cs.setCount(cs.getCount() + 1);
            statistics.put(sensorID, cs);
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, CountSum> pair : statistics.entrySet()) {
            context.write(new Text(pair.getKey()), pair.getValue());
        }
    }

}
