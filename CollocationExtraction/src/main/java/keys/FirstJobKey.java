package keys;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

public  class FirstJobKey implements WritableComparable<FirstJobKey> {


    private Text first ;
    private Text second ;
    private int decide ;

    private String asr = "*";

    public FirstJobKey() {
        this.first = new Text();
        this.second = new Text();
        decide = 0;
    }

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
    // (w1,*)>(w1,w2)>(*,w2)>(*,*)
    public int compareTo(FirstJobKey o) {
        int dec = this.decide - o.decide;
        int first = this.first.toString().compareTo(o.first.toString());
        int second = this.second.toString().compareTo(o.second.toString());
        if(dec == 0) {
            if (first == 0)
                return second;
            return first;
        }
        return dec;
//        if(o.first.toString().equals(asr) && o.second.toString().equals(asr)&&this.first.toString().equals(asr) && this.second.toString().equals(asr))
//            return 0;
//        //if o is <*,*> then he first
//        if (o.first.toString().equals(asr) && o.second.toString().equals(asr))
//            return 1;
//        //if this is <*,*> then he first
//        if (this.first.toString().equals(asr) && this.second.toString().equals(asr))
//            return -1;
//        // if  o is <*, >
//        if(o.first.toString().equals(asr)){
//            if(this.first.toString().equals(asr)){
//               return this.second.toString().compareTo(o.second.toString());
//            }else{ //  '<w1 , >'     >     '<* ,w2>'
//                return 1;
//            }
//        }else if (o.second.toString().equals(asr)){
//            if(this.first.toString().compareTo(o.first.toString()) == 0){ //  '<* ,w2 >'     <     '<W1 ,>'
//                return -1;
//            }else{
//                return this.first.toString().compareTo(o.first.toString());
//            }
//        }
//        else{
//            if(this.first.toString().equals(asr)){
//                return -1;
//            }else if(this.first.toString().compareTo(o.first.toString()) == 0){
//                return this.second.toString().equals(asr) ? 1:this.second.toString().compareTo(o.second.toString());
//            }else{
//                return this.first.toString().compareTo(o.first.toString());
//            }
//        }
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
        return first.toString().hashCode();
//        if(first.toString().equals(asr) && second.toString().equals(asr))
//            return asr.hashCode();
//        else if (first.toString().equals(asr))
//            return second.hashCode();
//        else
//            return first.hashCode();

    }

    @Override
    public String toString() {
        return this.first.toString() + " " + this.second.toString() + " " + this.decide;
    }

//    public static void main(String[] args) throws IOException {
//        double a = 10/Math.log10(1);
//        if (a > 1) a=1;
//        System.out.println(a);
//    }

}
