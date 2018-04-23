package exercise11;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountSum implements org.apache.hadoop.io.Writable {

    private float sum = 0;
    private int count = 0;

    public float getSum() {
        return sum;
    }

    public void setSum(float givenSum) {
        this.sum = givenSum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int givenCount) {
        this.count = givenCount;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sum = in.readFloat();
        this.count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.sum);
        out.writeInt(this.count);
    }

    public String toString() {
        return "" + this.sum / this.count;
    }

}
