package InputFormat;

import RecordReaders.SecondJobRecordReader;
import Values.FirstJobValue;
import keys.FirstJobKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class SecondJobInputFormat extends FileInputFormat<FirstJobKey, FirstJobValue> {
    @Override
    public RecordReader<FirstJobKey, FirstJobValue> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new SecondJobRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
}
