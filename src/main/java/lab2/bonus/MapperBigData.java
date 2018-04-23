package lab2.bonus;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<Text, Text, Text, Text> {

    private String search;

    protected void setup(Context context) {
        search = context.getConfiguration().get("search");
    }

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = key.toString().split(" ");
        if (words[0].equals(search) || words[1].equals(search))
            context.write(key, value);
    }

}
