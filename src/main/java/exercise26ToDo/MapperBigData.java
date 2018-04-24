package exercise26ToDo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class MapperBigData extends Mapper<LongWritable, Text, NullWritable, Text> {

    private HashMap<String, Integer> dictionary = new HashMap<>();

    @SuppressWarnings("deprecation")
    protected void setup(Context context) throws IOException {

        String row;
        /*
        It works in a hdfs only
        Path[] PathsCachedFiles = context.getLocalCacheFiles();
        BufferedReader fileStopWords = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));
        */
        BufferedReader fileStopWords = new BufferedReader(new FileReader(new File("/Users/davide/Documents/workspace/IdeaProjects/BigdataHadoop/dictionary.txt")));

        while ((row = fileStopWords.readLine()) != null) {
            String[] words = row.split("\t");
            dictionary.put(words[0], Integer.parseInt(words[0]));
        }
        fileStopWords.close();

    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String result = "";
        Integer intValue;
        String[] words = value.toString().split("\\s+");
        for (String word : words) {
            intValue = dictionary.get(word.toUpperCase());
            result = result.concat(intValue + " ");
        }
        context.write(NullWritable.get(), new Text(result));

    }

}
