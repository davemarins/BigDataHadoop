package exercise21;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.ArrayList;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

class MapperBigData extends Mapper<LongWritable, Text, NullWritable, Text> {

    private ArrayList<String> stopWords;

    @SuppressWarnings("deprecation")
    protected void setup(Context context) throws IOException {

        String nextLine;
        stopWords = new ArrayList<>();
        Path[] PathsCachedFiles = context.getLocalCacheFiles();
        BufferedReader fileStopWords = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));
        while ((nextLine = fileStopWords.readLine()) != null) {
            stopWords.add(nextLine);
        }
        fileStopWords.close();

    }


    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");
        String newSentence = "";
        for (String word : words) {
            if (!stopWords.contains(word)) {
                newSentence = newSentence.concat(word + " ");
            }
        }
        context.write(NullWritable.get(), new Text(newSentence));

    }
}
