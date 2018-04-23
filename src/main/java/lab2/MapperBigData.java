package lab2;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<Text, Text, Text, Text> {

    private String prefix;

    protected void setup(Context context) {
        prefix = context.getConfiguration().get("prefix");
    }

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (key.toString().startsWith(prefix))
            context.write(key, value);
    }

}
