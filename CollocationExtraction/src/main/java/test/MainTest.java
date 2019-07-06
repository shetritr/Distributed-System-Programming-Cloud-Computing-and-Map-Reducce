package test;


import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;


public class MainTest {
    public static void main(String[] args) throws IOException {

        String input = "s3n://fortestrefael/input.txt";
        String output = "s3n://fortestrefael/output/";
        UUID uuid = UUID.randomUUID();
        // get AWS credentials
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(credentialsProvider)
                .build();


        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
               .withJar("s3n://fortestrefael/Job1.jar") // This should be a full map reduce application.
                .withArgs(input, output , uuid.toString());

        StepConfig stepConfig1 = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://fortestrefael/Job2.jar") // This should be a full map reduce application.
                .withArgs(uuid.toString());

        StepConfig stepConfig2 = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(hadoopJarStep2)
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
                .withName("FirstJob")
                .withReleaseLabel("emr-5.14.0")
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2)
                .withLogUri("s3n://fortestrefael/logs/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}

