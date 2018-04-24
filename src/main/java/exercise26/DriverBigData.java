package exercise26;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

        int exitCode;
        Path inputPath = new Path(args[0]), outputDir = new Path(args[1]);
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        // job.addCacheFile(new Path("/Users/davide/Documents/workspace/IdeaProjects/BigdataHadoop/dictionary.txt").toUri());

        job.setJobName("Exercise 26 - String to Integer conversion");
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(DriverBigData.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MapperBigData.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        if (job.waitForCompletion(true)) {
            exitCode = 0;
        } else {
            exitCode = 1;
        }

        return exitCode;
    }

}
