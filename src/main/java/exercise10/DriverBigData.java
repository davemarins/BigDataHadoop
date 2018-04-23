package exercise10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverBigData extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);
        System.exit(res);
    }

    @Override
    public int run(String args[]) throws Exception {

        Path inputPath, outputDirectory;
        // int numberOfReducers;
        int exitCode;

        // numberOfReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputDirectory = new Path(args[2]);
        // Configuration conf = this.getConf(); // not necessary
        Job job = Job.getInstance();

        job.setJobName("Exercise 10 - Total count");
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDirectory);

        job.setJarByClass(DriverBigData.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MapperBigData.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(/*numberOfReducers*/ 0);

        if (job.waitForCompletion(true)) {
            Counter totalRecords = job.getCounters().findCounter(MY_COUNTERS.TOTAL_RECORDS);
            System.out.println("Total: " + totalRecords.getValue());
            exitCode = 0;
        } else {
            exitCode = 1;
        }
        return exitCode;

    }

    public enum MY_COUNTERS {
        TOTAL_RECORDS
    }

}
