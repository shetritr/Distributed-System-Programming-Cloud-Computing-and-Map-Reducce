package Values;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FirstJobValue implements Writable {
    long CW1W2;//  Num of C(W1,W2) , if (*,w2) CW1W2 = 0
    long CW1N;// Num of C(W1) , if (*,w2) CW1N = Cw2

    public FirstJobValue(long CW1W2, long CW1N) {
        this.CW1W2 = CW1W2;
        this.CW1N = CW1N;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(CW1W2);
        out.writeLong(CW1N);
    }

    public void readFields(DataInput in) throws IOException {
        CW1W2 = in.readLong();
        CW1N = in.readLong();
    }

    public long getCW1W2() {
        return CW1W2;
    }

    public long getCW1N() {
        return CW1N;
    }

    @Override
    public String toString() {
        return this.CW1W2 + " " + this.CW1N;
    }
}
