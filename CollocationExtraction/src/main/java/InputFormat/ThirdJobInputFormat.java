package InputFormat;

import RecordReaders.ThirdJobRecordReader;
import Values.SecondJobValueReduce;
import keys.FirstJobKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class ThirdJobInputFormat extends FileInputFormat<FirstJobKey, SecondJobValueReduce> {
    @Override
    public org.apache.hadoop.mapreduce.RecordReader<FirstJobKey, SecondJobValueReduce> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new ThirdJobRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
}
