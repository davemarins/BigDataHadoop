package exercise21;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

class MapperBigData extends Mapper<LongWritable, Text, NullWritable, Text> {

    private ArrayList<String> stopWords = new ArrayList<>();

    @SuppressWarnings("deprecation")
    protected void setup(Context context) throws IOException {

        String nextLine;
        /*
        It works in a hdfs only
        Path[] PathsCachedFiles = context.getLocalCacheFiles();
        BufferedReader fileStopWords = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));
        */
        BufferedReader fileStopWords = new BufferedReader(new FileReader(new File("/Users/davide/Documents/workspace/IdeaProjects/BigdataHadoop/stopwords.txt")));
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
