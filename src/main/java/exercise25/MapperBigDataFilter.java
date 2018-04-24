package exercise25;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperBigDataFilter extends Mapper<Text, Text, Text, Text> {

    protected void map(Text key, Text values, Context context) throws IOException, InterruptedException {
        String[] users = values.toString().split(",");
        for (String user : users) {
            context.write(key, new Text(user));
        }
    }

}
