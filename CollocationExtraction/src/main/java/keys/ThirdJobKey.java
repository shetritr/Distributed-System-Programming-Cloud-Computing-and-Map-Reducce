package keys;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ThirdJobKey implements WritableComparable<ThirdJobKey> {


    private Text first ;
    private Text second ;
    private int decide ;

    private String asr = "*";

    public ThirdJobKey() {
        this.first = new Text();
        this.second = new Text();
        decide = 0;
    }

    public ThirdJobKey(String first, String second, int decide) {
        set(new Text(first),new Text(second) , decide);
    }

    public void set(Text first, Text second, int decide) {
        this.first = first;
        this.second = second;
        this.decide = decide;
    }

    public Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public Text getSecond() {
        return second;
    }

    public void setSecond(Text second) {
        this.second = second;
    }

    public int getDecide() {
        return decide;
    }

    public void setDecide(int decide) {
        this.decide = decide;
    }

    // 1 if this greater then o
    // -1 if the second
    // else 0
    // (w1,*)>(w1,w2)>(*,w2)>(*,*)
    public int compareTo(ThirdJobKey o) {
        int dec = this.decide - o.decide;
        int first = this.first.toString().compareTo(o.first.toString());
        int second = this.second.toString().compareTo(o.second.toString());
        if(dec == 0) {
            if (first == 0)
                return second;
            return first;
        }
        return dec;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(first.toString());
        out.writeUTF(second.toString());
        out.writeInt(decide);
    }

    public void readFields(DataInput in) throws IOException {
        this.first = new Text(in.readUTF());
        this.second = new Text(in.readUTF());
        decide = in.readInt();
    }

    @Override
    public int hashCode() {
        return decide;
    }

    @Override
    public String toString() {
        return this.first.toString() + " " + this.second.toString() + " " + this.decide;
    }
}
