package exercise25;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
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

        Path inputPath = new Path(args[1]), outputDir = new Path(args[2]), outputDir2 = new Path(args[3]);
        int exitCode, numberOfReducers = Integer.parseInt(args[0]);
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);

        job.setJobName("Exercise 25 - Compute the list of potential friends of a specific user");

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(DriverBigData.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MapperBigData.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ReducerBigData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(numberOfReducers);

        if (job.waitForCompletion(true)) {

            Configuration conf2 = this.getConf();
            Job job2 = Job.getInstance(conf2);
            job2.setJobName("Exercise 25 - Compute the list of potential friends of a specific user");

            FileInputFormat.addInputPath(job2, outputDir);
            FileOutputFormat.setOutputPath(job2, outputDir2);

            job2.setJarByClass(DriverBigData.class);
            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.setMapperClass(MapperBigDataFilter.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setReducerClass(ReducerBigDataFilter.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            job2.setNumReduceTasks(1);

            if (job2.waitForCompletion(true)) {
                exitCode = 0;
            } else {
                exitCode = 1;
            }

        } else {
            exitCode = 1;
        }
        return exitCode;
    }
}
