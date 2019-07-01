package Jobs;

import Values.firstJobValue;
import keys.FirstJobKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class firstJob {


    public final static Text asr = new Text("*");


    public static class firstMapper extends Mapper<Object, Text, FirstJobKey, LongWritable> {

        public void map(Object key,Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());// split the value by " \t\n\r\f"
            if (!st.hasMoreTokens()) return;
            String W1 = st.nextToken().toLowerCase(); // word 1
            if (!st.hasMoreTokens()) return;
            String W2 = st.nextToken().toLowerCase();
            if (!st.hasMoreTokens()) return; // word 2
            int dec = Integer.parseInt(st.nextToken());
            dec = dec - dec%10; // calc the decide
            if (!st.hasMoreTokens()) return;
            Long count  = Long.valueOf(st.nextToken());

            // for each pair we write 4 <key,Value>
            // 1. for count the N 2-gram we write <*,*>
            // 2. for count the C(W1) we write <w1,*>
            // 3. for count the C(W2) we write <*,W2>
            // 4. for count the C(W1,W2) we write <W1,W2>

            // 1
            context.write(new FirstJobKey(firstJob.asr.toString(),firstJob.asr.toString(),dec),new LongWritable(1));

            // 2
            context.write(new FirstJobKey(W1,firstJob.asr.toString(),dec),new LongWritable(count));

            //3
            context.write(new FirstJobKey(firstJob.asr.toString(),W2,dec),new LongWritable(count));

            //4
            context.write(new FirstJobKey(W1,W2,dec),new LongWritable(count));


        }
    }



    public static class firstCombiner extends Reducer<FirstJobKey,LongWritable,FirstJobKey,LongWritable> {
        @Override
        public void reduce(FirstJobKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long Counter = 0;

            for (LongWritable value : values) {
                Counter += value.get();
            }

            context.write(key, new LongWritable(Counter));
        }


    }

    public static class firstReducer extends Reducer<FirstJobKey,LongWritable,FirstJobKey, firstJobValue> {
//        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
//        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
//                .withCredentials(credentialsProvider)
//                .withRegion(Regions.US_EAST_1)
//                .build();
        long CW1 = 0;
        Text CurrW =new Text("");

        @Override
        public void reduce(FirstJobKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long Counter = 0;

            for (LongWritable value : values) {
                Counter += value.get();
            }


            // in this step we save the (*,*) because we get it first we can save it
            if(key.getFirst().equals(asr) && key.getSecond().equals(asr)){
                CW1 = 0;                                                         // Key    Value
                context.write(key,new firstJobValue(0,Counter)); // write <(*,*) , (0,N)>
            }// if we in (w1,*) we save the counter in field , because we sorting the data, we know that (W1,*) cam before (W1,W2)
            else if (key.getSecond().equals(asr)){
                if(key.getFirst().equals(CurrW)){
                    System.err.println("There is 2 (W1,*), Error");
                    return;
                }

                CW1 = Counter;
                CurrW = key.getFirst();
            }// count the (*,W2) , for C(W2)
            else if(key.getFirst().equals(asr)){
                                                                        //         Key      Value
                context.write(key,new firstJobValue(0,Counter));//write <(*,W2) , (0,C(W2)>
            }else{
                                                                   //        Key           Value
                context.write(key,new firstJobValue(Counter,CW1));//write <(W1,W2) , (C(W1,W2),C(W1)>
            }

        }


    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String [] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Usage: WordCount <input_file> <output_file>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"firstJob");
        job.setJarByClass(firstJob.class);//Jar class
        job.setMapperClass(firstJob.firstMapper.class);//mapper
        job.setReducerClass(firstJob.firstReducer.class);//reducer
        job.setCombinerClass(firstJob.firstCombiner.class);//combiner
        job.setNumReduceTasks(5);//how many reduce tasks that we want
        job.setOutputKeyClass(FirstJobKey.class);
        job.setOutputValueClass(firstJobValue.class);

        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        boolean status = job.waitForCompletion(true);
        if(status){
            System.exit(0);
        }else{
            System.exit(1);
        }
    }

}
