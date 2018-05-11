package lab3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCountWritable implements Comparable<WordCountWritable>, Writable {

    private String word;
    private Integer count;

    WordCountWritable(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public int compareTo(WordCountWritable wcw) {
        if (this.count.compareTo(wcw.getCount()) != 0) {
            return this.count.compareTo(wcw.getCount());
        } else {
            return this.word.compareTo(wcw.getWord());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(count);
    }

    public String toString() {
        return this.word + " => " + this.count;
    }

}
