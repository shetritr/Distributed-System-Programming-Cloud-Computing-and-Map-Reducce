package test;


import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;


public class LoaclApp {


    private static AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

    private static void PutOnS3(String directoryName,String bucketName){
        s3.createBucket(bucketName);
        File dir = new File(directoryName);
        for (File file : dir.listFiles()) {
            String key = file.getName().replace('\\', '_').replace('/','_').replace(':', '_');
            PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
            s3.putObject(req);
        }
    }

    public static void main(String[] args) throws IOException {


        UUID uuid = UUID.randomUUID();
        // get AWS credentials
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
//        System.out.println(new File( "target/Job1.jar" ).length());

        //
        String credent = credentialsProvider.getCredentials().getAWSAccessKeyId();
        String add = ("Refael-And-Mhor-"+credent).toLowerCase();
        String input ="s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";// "s3n://" + add + "/input.txt";

        PutOnS3("uploads",add);

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(credentialsProvider)
                .build();


        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
               .withJar("s3n://"+add+"/Job1.jar") // This should be a full map reduce application.
                .withArgs(add , input ,uuid.toString());

        StepConfig stepConfig1 = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://"+add+"/Job2.jar") // This should be a full map reduce application.
                .withArgs(add,uuid.toString());

        StepConfig stepConfig2 = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://"+add+"/Job3.jar") // This should be a full map reduce application.
                .withArgs(add,uuid.toString(),args[0],args[1]);

        StepConfig stepConfig3 = new StepConfig()
                .withName("step3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M1Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M1Xlarge.toString())
                .withHadoopVersion("2.2.0")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withEc2KeyName("demo")
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("EX2")
                .withReleaseLabel("emr-5.14.0")
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2,stepConfig3)
                .withLogUri("s3n://"+add+"/log")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}

