package exercise8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    public static void main(String args[]) throws Exception {

        int res = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath;
        Path outputDirStep1;
        Path outputDirStep2;
        int numberOfReducers;
        int exitCode;

        numberOfReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputDirStep1 = new Path(args[2]);
        outputDirStep2 = new Path(args[3]);

        Configuration conf = this.getConf();
        Job job1 = Job.getInstance(conf);
        job1.setJobName("Exercise 8 - Total income for each month of the year");

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputDirStep1);

        job1.setJarByClass(Driver.class);
        job1.setInputFormatClass(KeyValueTextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setMapperClass(Mapper1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setNumReduceTasks(numberOfReducers);

        if (job1.waitForCompletion(true)) {
            exitCode = 0;
        } else {
            exitCode = 1;
        }

        if (exitCode == 0) {

            Job job2 = Job.getInstance(conf);
            job2.setJobName("Exercise 8 - Average monthly income per year");

            FileInputFormat.addInputPath(job2, outputDirStep1);
            FileOutputFormat.setOutputPath(job2, outputDirStep2);

            job2.setJarByClass(Driver.class);
            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            job2.setMapperClass(Mapper2.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(DoubleWritable.class);
            job2.setReducerClass(Reducer2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            job2.setNumReduceTasks(numberOfReducers);

            if (job2.waitForCompletion(true)) {
                exitCode = 0;
            } else {
                exitCode = 1;
            }

        }

        return exitCode;

    }
}
