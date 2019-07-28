package Jobs;

import InputFormat.SecondJobInputFormat;
import Values.FirstJobValue;
import Values.SecondJobValueMapper;
import Values.SecondJobValueReduce;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import keys.FirstJobKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class SecondJob {

    public static class SecondMapper extends Mapper<FirstJobKey, FirstJobValue, FirstJobKey, SecondJobValueMapper>{
        private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        private String asr = "*";
        private ConcurrentHashMap <String,Long> NsMap = new ConcurrentHashMap<String,Long> ();

        @Override
        protected void map(FirstJobKey key, FirstJobValue value, Context context) throws IOException, InterruptedException {
            if(!NsMap.containsKey(String.valueOf(key.getDecide()))){
                UpdateMap(context.getConfiguration().get("NsPath"),String.valueOf(key.getDecide()));
            }

            // now switch (w1,w2) => (w2,w1) ,for sorting by w2
            if(key.getFirst().toString().equals(asr)){  // when (*,w2)
                context.write(new FirstJobKey(key.getSecond().toString(),key.getFirst().toString(),key.getDecide()),new SecondJobValueMapper(value.getCW1N()));
            }
            else{ // when (w1,w2)
                context.write(new FirstJobKey(key.getSecond().toString(),key.getFirst().toString(),key.getDecide()),new SecondJobValueMapper(value.getCW1W2(),value.getCW1N(),NsMap.get(String.valueOf(key.getDecide()))));
            }

        }

        public void UpdateMap(String path, String FileName) throws IOException {
            S3Object object = s3.getObject(new GetObjectRequest(path, FileName));
            long N_dec = readTextInputStream(object.getObjectContent());
            NsMap.put(FileName,N_dec);
        }
    }

    public static class secondReducer extends Reducer<FirstJobKey, SecondJobValueMapper,FirstJobKey, SecondJobValueReduce> {

        private FirstJobKey CurrFirstJobKey = new FirstJobKey();
        private SecondJobValueMapper currSecondJobValue = new SecondJobValueMapper();
        private String asr = "*";


        @Override
        protected void reduce(FirstJobKey key, Iterable<SecondJobValueMapper> values, Context context) throws IOException, InterruptedException {

            SecondJobValueMapper secondjobvalue;

            if(values.iterator().hasNext()){
                secondjobvalue = values.iterator().next();
            }else{
                System.err.println("Iterable<SecondJobValueMapper> values is empty, Error");
                return;
            }
            // if (w2,*) save the value  of the counter , for C(W2)
            if (key.getSecond().toString().equals(asr)){
//                if(key.getFirst().toString().equals(CurrFirstJobKey.getFirst()) && key.getDecide() == CurrFirstJobKey.getDecide()){
//                    System.err.println("There is 2 (W1,*), Error");
//                    return;
//                }
                currSecondJobValue = secondjobvalue;

            }else{
                context.write(new FirstJobKey(key.getSecond().toString(),key.getFirst().toString(),key.getDecide()),
                        new SecondJobValueReduce(secondjobvalue.getCW1W2(),secondjobvalue.getCW1N(),currSecondJobValue.getCW1W2(),secondjobvalue.getCN()));
            }

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String add = args[0];
        String uuid = args[1];
        String NsPath =add + "/temp/" + uuid + "/Ns";
        String input = "s3n://"+add+"/steps/" + uuid + "/Job1";
        String output = "s3n://"+add+"/steps/" + uuid + "/Job2";

        Configuration conf = new Configuration();
        conf.set("NsPath", NsPath);
        Job job = Job.getInstance(conf,"SecondJob");
        job.setJarByClass(SecondJob.class);//Jar class
        job.setMapperClass(SecondJob.SecondMapper.class);//mapper
        job.setReducerClass(SecondJob.secondReducer.class);//reducer
        job.setMapOutputKeyClass(FirstJobKey.class);
        job.setMapOutputValueClass(SecondJobValueMapper.class);
        job.setOutputKeyClass(FirstJobKey.class);
        job.setOutputValueClass(SecondJobValueReduce.class);
        job.setInputFormatClass(SecondJobInputFormat.class);

        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        boolean status = job.waitForCompletion(true);
        if(status){
            System.exit(0);
        }else{
            System.exit(1);
        }
    }
    private static long readTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            return  Long.parseLong(line);
        }
        return 0;
    }
}
