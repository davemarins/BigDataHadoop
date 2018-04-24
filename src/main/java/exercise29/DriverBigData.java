package exercise29;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
    public int run(String[] args) throws Exception {

        int exitCode, numberOfReducers = Integer.parseInt(args[0]);
        Path inputPath1 = new Path(args[1]), inputPath2 = new Path(args[2]);
        Path outputDir = new Path(args[3]);
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("Exercise 29 - User selection");
        MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, Mapper1BigData.class);
        MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, Mapper2BigData.class);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(DriverBigData.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ReducerBigData.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(numberOfReducers);

        if (job.waitForCompletion(true)) {
            exitCode = 0;
        } else {
            exitCode = 1;
        }

        return exitCode;

    }
}
