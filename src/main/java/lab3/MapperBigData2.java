package lab3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData2 extends Mapper<Text, Text, NullWritable, WordCountWritable> {

    private Integer k = 100;
    private TopKVector<WordCountWritable> localTopK = new TopKVector<>(k);

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        WordCountWritable pair = new WordCountWritable(key.toString(), Integer.parseInt(value.toString()));
        this.localTopK.updateWithNewElement(pair);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (WordCountWritable wcw : localTopK.getLocalK()) {
            context.write(NullWritable.get(), wcw);
        }
    }
}
