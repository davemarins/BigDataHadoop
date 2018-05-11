package lab3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

        Configuration conf = this.getConf();
        Job job1 = Job.getInstance(conf);
        job1.setJobName("Lab3 - Step 1");

        Path inputPath = new Path(args[2]), outputDir1 = new Path(args[3]), outputDir2 = new Path(args[4]);
        int numberOfReducersJob1 = Integer.parseInt(args[0]), numberOfReducersJob2 = Integer.parseInt(args[1]);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputDir1);

        job1.setJarByClass(DriverBigData.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapperClass(MapperBigData1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setReducerClass(ReducerBigData1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setNumReduceTasks(numberOfReducersJob1);

        if (job1.waitForCompletion(true)) {

            Job job2 = Job.getInstance(conf);
            job2.setJobName("Lab3 - Step 2");

            FileInputFormat.addInputPath(job2, outputDir1);
            FileOutputFormat.setOutputPath(job2, outputDir2);

            job2.setJarByClass(DriverBigData.class);
            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.setMapperClass(MapperBigData2.class);
            job2.setMapOutputKeyClass(NullWritable.class);
            job2.setMapOutputValueClass(WordCountWritable.class);

            job2.setReducerClass(ReducerBigData2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            job2.setNumReduceTasks(numberOfReducersJob2);

            if (job2.waitForCompletion(true)) {
                return 0;
            } else {
                return 1;
            }

        } else {
            return 1;
        }

    }
}
