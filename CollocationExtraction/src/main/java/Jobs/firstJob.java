package Jobs;

import Values.FirstJobValue;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import keys.FirstJobKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.StringTokenizer;

public class FirstJob {

    public static class firstMapper extends Mapper<Object, Text, FirstJobKey, LongWritable> {

        private Text asr = new Text("*");


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());// split the value by " \t\n\r\f"
            if (!st.hasMoreTokens()) return;
            String W1 = st.nextToken().toLowerCase(); // word 1
            if (!st.hasMoreTokens()) return;
            String W2 = st.nextToken().toLowerCase();
            if (!st.hasMoreTokens()) return; // word 2
            int dec = Integer.parseInt(st.nextToken());
            int dec2 = dec - dec%10; // calc the decide
            if (!st.hasMoreTokens()) return;
            long count  = Long.valueOf(st.nextToken());

            // for each pair we write 4 <key,Value>
            // 1. for count the N 2-gram we write <*,*>
            // 2. for count the C(W1) we write <w1,*>
            // 3. for count the C(W2) we write <*,W2>
            // 4. for count the C(W1,W2) we write <W1,W2>

            // 1
            context.write(new FirstJobKey(asr.toString(),asr.toString(),dec2),new LongWritable(count));

            // 2
            context.write(new FirstJobKey(W1,asr.toString(),dec2),new LongWritable(count));

            //3
            context.write(new FirstJobKey(asr.toString(),W2,dec2),new LongWritable(count));

            //4
            context.write(new FirstJobKey(W1,W2,dec2),new LongWritable(count));
        }
    }

    public static class firstPartitioner extends Partitioner< FirstJobKey, LongWritable> {

        @Override
        public int getPartition(FirstJobKey key, LongWritable value, int num) {
            return Math.abs(key.hashCode()) % num;
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

    public static class firstReducer extends Reducer<FirstJobKey,LongWritable,FirstJobKey, FirstJobValue> {

        private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

       private FirstJobKey CurrFirstJobKey =new FirstJobKey();
       private long CW1 = 0;

        String asr = "*";

        @Override
        public void reduce(FirstJobKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long Counter = 0;

            for (LongWritable value : values) {
                Counter += value.get();
            }
//            context.write(key,new FirstJobValue(0,Counter));//write <(*,W2) , (0,C(W2)>


            // in this step we save the (*,*) in S3
            if(key.getFirst().toString().equals(asr) && key.getSecond().toString().equals(asr)){

                String path = context.getConfiguration().get("TemPath");
                String file = String.valueOf(Counter);
                InputStream is = new ByteArrayInputStream( file.getBytes());
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(file.getBytes().length);
                String resultFileName = String.valueOf(key.getDecide());
                PutObjectRequest req = new PutObjectRequest(path, resultFileName, is ,metadata);
                s3.putObject(req);
                CW1 = 0;                                                         // Key    Value
//                context.write(key,new FirstJobValue(0,Counter)); // write <(*,*) , (0,N)>
            }// if we in (w1,*) we save the counter in field , because we sorting the data, we know that (W1,*) cam before (W1,W2)
            else if (key.getSecond().toString().equals(asr)){
//                if(key.getFirst().toString().equals(CurrFirstJobKey.getFirst().toString()) && key.getDecide() == CurrFirstJobKey.getDecide()){
//                    System.err.println("There is 2 (W1,*), Error");
//                    return;
//                }
                CW1 = Counter;
                CurrFirstJobKey = key;
//                context.write(key,new FirstJobValue(0,Counter));//write <(*,W2) , (0,C(W2)>
            }// count the (*,W2) , for C(W2)
            else if(key.getFirst().toString().equals(asr)){
            //                                                                     Key      Value
                context.write(key,new FirstJobValue(0,Counter));//write <(*,W2) , (0,C(W2)>
            }else{
            //                                                               Key           Value
                context.write(key,new FirstJobValue(Counter,CW1));//write <(W1,W2) , (C(W1,W2),C(W1)>
            }

        }


    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String add = args[0];
        String input = args [1];
        String uuid = args[2];

        String output = "s3n://" + add + "/steps/" + uuid + "/Job1";
        String TemPath =add +"/temp/" + uuid + "/Ns";
        Configuration conf = new Configuration();
        conf.set("TemPath", TemPath);
        Job job = Job.getInstance(conf,"FirstJob");
        job.setJarByClass(FirstJob.class);//Jar class
        job.setPartitionerClass(firstPartitioner.class);
        job.setMapperClass(firstMapper.class);//mapper
        job.setReducerClass(FirstJob.firstReducer.class);//reducer
        job.setCombinerClass(FirstJob.firstCombiner.class);//combiner
        job.setNumReduceTasks(4);//how many reduce tasks that we want
        job.setMapOutputKeyClass(FirstJobKey.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(FirstJobKey.class);
        job.setOutputValueClass(FirstJobValue.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

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
