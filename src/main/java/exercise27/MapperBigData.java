package exercise27;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class MapperBigData extends Mapper<LongWritable, Text, NullWritable, Text> {

    private ArrayList<String> rules = new ArrayList<>();

    @SuppressWarnings("deprecation")
    protected void setup(Context context) throws IOException {

        String row;
        /*
        Path[] PathsCachedFiles = context.getLocalCacheFiles();
        BufferedReader rulesFile = new BufferedReader(new FileReader(new File(PathsCachedFiles[0].toString())));
        */
        BufferedReader readRules = new BufferedReader(new FileReader(new File("/Users/davide/Documents/workspace/IdeaProjects/BigdataHadoop/businessrules-es27.txt")));
        while ((row = readRules.readLine()) != null) {
            this.rules.add(row);
        }
        readRules.close();

    }

    private String businessRule(String gender, String birthYear) {
        String category = "Unknown";
        for (String rule : this.rules) {
            String[] fields = rule.split(" ");
            // [0] Gender=M; [2] YearOfBirth=1934; [4] Category#1
            if (fields[0].compareTo("Gender=" + gender) == 0 && fields[2].compareTo("YearOfBirth=" + birthYear) == 0) {
                category = fields[4];
                break;
            }
        }
        return category;
    }

    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

        String[] fields = values.toString().split(",");
        String category = businessRule(fields[3], fields[4]);
        context.write(NullWritable.get(), new Text(values.toString() + "," + category));

    }

}
