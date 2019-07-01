package keys;

import Jobs.firstJob;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FirstJobKey implements WritableComparable {


    private Text first;
    private Text second;
    private int decide;

    
    public FirstJobKey(String first, String second, int decide) {
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
    // (*,*)>(w1,*)>(w1,w2)>(*,w2)
    public int compareTo(FirstJobKey o) {
        //if o is <*,*> then he first
        if (o.first.equals(firstJob.asr) && o.second.equals(firstJob.asr))
            return -1;
        //if this is <*,*> then he first
        if (this.first.equals(firstJob.asr) && this.second.equals(firstJob.asr))
            return 1;
        // if  o is <*, >
        if(o.first.equals(firstJob.asr)){
            if(this.first.equals(firstJob.asr)){
               return this.second.compareTo(o.second);
            }else{ //  '<w1 , >'     >     '<* ,w2>'
                return 1;
            }
        }else if (o.second.equals(firstJob.asr)){
            if(this.first.compareTo(o.first) == 0){ //  '<* ,w2 >'     <     '<W1 ,>'
                return -1;
            }else{
                return this.first.compareTo(o.first);
            }
        }
        else{
            if(this.first.equals(firstJob.asr)){
                return -1;
            }else if(this.first.compareTo(o.first) == 0){
                return this.second.equals(firstJob.asr) ? 1:this.second.compareTo(o.second);
            }else{
                return this.first.compareTo(o.first);
            }
        }
    }

    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        out.writeLong(decide);
    }

    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        decide = in.readInt();
    }

    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public int hashCode() {
        if(first.toString().equals(firstJob.asr) && second.toString().equals(firstJob.asr))
            return firstJob.asr.hashCode();
        else if (first.toString().equals(firstJob.asr))
            return second.hashCode();
        else
            return first.hashCode();

    }

    //    public static void main(String[] args) {
//        FirstJobKey firstJobKey = new FirstJobKey("*","b",3);
//        FirstJobKey firstJobKey1 = new FirstJobKey("*","a",3);
//        System.out.println(firstJobKey.compareTo(firstJobKey1));
//    }
}
