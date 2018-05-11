package exercise26;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

// import org.apache.hadoop.fs.Path;

class MapperBigData extends Mapper<LongWritable, Text, NullWritable, Text> {

    private HashMap<String, Integer> dictionary = new HashMap<>();

    protected void setup(Context context) throws IOException {

        String line;

        // Path[] PathsCachedFiles = context.getLocalCacheFiles();
        // BufferedReader stopWords = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));

        String path = context.getConfiguration().get("dictionary");
        BufferedReader stopWords = new BufferedReader(new FileReader(new File(path)));

        while ((line = stopWords.readLine()) != null) {
            String[] record = line.split("\t");
            Integer intValue = Integer.parseInt(record[0]);
            dictionary.put(record[1], intValue);
        }
        stopWords.close();

    }


    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String convertedString = "";
        String[] words = value.toString().split("\\s+");
        for (String word : words) {
            Integer intValue = dictionary.get(word.toUpperCase());
            convertedString = convertedString.concat(intValue + " ");
        }
        context.write(NullWritable.get(), new Text(convertedString));

    }
}
