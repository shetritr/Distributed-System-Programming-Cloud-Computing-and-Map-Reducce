package Jobs;

import InputFormat.ThirdJobInputFormat;
import Values.SecondJobValueReduce;
import keys.FirstJobKey;
import keys.ThirdJobKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ThirdJob {

    public static class ThirdMapper  extends Mapper<FirstJobKey, SecondJobValueReduce, ThirdJobKey, DoubleWritable>{

        private String asr = "*";
        @Override
        protected void map(FirstJobKey key, SecondJobValueReduce value, Context context) throws IOException, InterruptedException {

            //for -log[p(w1,w2)]
            double Pw1w2 =(double)value.getCW1W2()/value.getCN();

            //pmi(w1,w2) =log(N) + log(c(w1,w2)) - log(c(w1)) - log(c(w2))
            double pmi = Math.log10(value.getCW1W2()) + Math.log10(value.getCN()) - Math.log10(value.getCW1N()) - Math.log10(value.getCW2N());

            // Npmi(w1,w2) = pmi(w1,w2)/-log[p(w1,w2)]
            double Npmi = pmi/(-Math.log10(Pw1w2));

            if (Npmi > 1) Npmi = 1;

            // one for Decide counting
            context.write(new ThirdJobKey(asr,asr,key.getDecide(),0),new DoubleWritable(Npmi));

            // one for bigram counting
            context.write(new ThirdJobKey(key.getFirst().toString(),key.getSecond().toString(),key.getDecide(),Npmi),new DoubleWritable(Npmi));

        }
    }

    public static class thirdPartitioner extends Partitioner<ThirdJobKey, DoubleWritable> {

        @Override
        public int getPartition(ThirdJobKey key, DoubleWritable value, int num) {
            return Math.abs(key.hashCode()) % num;
        }
    }

    public static class thirdCombiner extends Reducer<ThirdJobKey, DoubleWritable,ThirdJobKey, DoubleWritable> {
        @Override
        public void reduce(ThirdJobKey key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
            double Counter = 0;

            for (DoubleWritable value : values) {
                Counter += value.get();
            }

            context.write(key, new DoubleWritable(Counter));
        }


    }

    public static class thirdReducer extends Reducer<ThirdJobKey, DoubleWritable,ThirdJobKey, Text>{

        private double Minimal_pmi;
        private double Relative_minimal_pmi;
        private double Curr_Relative_minimal_pmi = 10000;
        private String asr = "*";
        private Text text = new Text("");

        @Override
        protected void reduce(ThirdJobKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double Counter = 0;

            for (DoubleWritable value : values) {
                Counter += value.get();
            }
            Minimal_pmi = Double.valueOf(context.getConfiguration().get("Minimal_pmi"));
            Relative_minimal_pmi = Double.valueOf(context.getConfiguration().get("Relative_minimal_pmi"));

            if(key.getFirst().toString().equals(asr)){
                Curr_Relative_minimal_pmi = Counter;
            }else {
                double calc_Relative_minimal_pmi = Counter/this.Curr_Relative_minimal_pmi;
                text.set(String.valueOf(Counter));
                if(Counter>Minimal_pmi)
                    context.write(key,text);
                else if(calc_Relative_minimal_pmi>Relative_minimal_pmi)
                    context.write(key,text);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        String add = args[0];
        String uuid = args[1];
        String Minimal_pmi = args[2];
        String Relative_minimal_pmi = args[3];

        String output = "s3n://"+add+"/Output";
        String input = "s3n://"+add+"/steps/" + uuid + "/Job2";
        Configuration conf = new Configuration();
        conf.set("Minimal_pmi", Minimal_pmi);
        conf.set("Relative_minimal_pmi", Relative_minimal_pmi);
        Job job = Job.getInstance(conf,"ThirdJob");
        job.setJarByClass(ThirdJob.class);//Jar class
        job.setPartitionerClass(ThirdJob.thirdPartitioner.class);
        job.setMapperClass(ThirdJob.ThirdMapper.class);//mapper
        job.setReducerClass(ThirdJob.thirdReducer.class);//reducer
        job.setCombinerClass(ThirdJob.thirdCombiner.class);//combiner
        job.setMapOutputKeyClass(ThirdJobKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(ThirdJobKey.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(ThirdJobInputFormat.class);
        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        boolean status = job.waitForCompletion(true);
        if(status){
            System.exit(0);
        }else{
            System.exit(1);
        }
    }

}
