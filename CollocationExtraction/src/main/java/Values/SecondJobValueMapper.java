package Values;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondJobValueMapper implements Writable {
    long CW1W2;//  Num of C(W1,W2)
    long CW1N;// Num of C(W1)
    long CN;// Num of C(N)

    public SecondJobValueMapper() {
        this.CW1W2 = 0;
        this.CW1N = 0;
        this.CN = 0;
    }

    public SecondJobValueMapper(long CW1W2, long CW1N , long CN) {
        this.CW1W2 = CW1W2;
        this.CW1N = CW1N;
        this.CN = CN;
    }

    public SecondJobValueMapper(long CW1W2) {
        this.CW1W2 = CW1W2;
        this.CW1N =0;
        this.CN = 0;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(CW1W2);
        out.writeLong(CW1N);
        out.writeLong(CN);
    }

    public long getCW1W2() {
        return CW1W2;
    }

    public long getCW1N() {
        return CW1N;
    }

    public long getCN() {
        return CN;
    }

    public void readFields(DataInput in) throws IOException {
        CW1W2 = in.readLong();
        CW1N = in.readLong();
        CN = in.readLong();
    }

    @Override
    public String toString() {
        return this.CW1W2 + " " + this.CW1N + " " + this.CN;
    }
}
