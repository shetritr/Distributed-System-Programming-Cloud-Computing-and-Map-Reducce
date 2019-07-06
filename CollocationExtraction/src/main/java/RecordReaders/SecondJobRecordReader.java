package RecordReaders;

import Values.FirstJobValue;
import keys.FirstJobKey;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.StringTokenizer;

public class SecondJobRecordReader extends RecordReader<FirstJobKey, FirstJobValue> {

    FirstJobValue value;
    FirstJobKey key;
    LineRecordReader reader;

    public SecondJobRecordReader() {
        reader = new LineRecordReader();
    }

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!reader.nextKeyValue()){
            return false;
        }
        try {
            StringTokenizer line = new StringTokenizer(reader.getCurrentValue().toString());// split the value by " \t\n\r\f"
            key = new FirstJobKey(line.nextToken(),line.nextToken(),Integer.parseInt(line.nextToken()));
            value = new FirstJobValue(Long.parseLong(line.nextToken()),Long.parseLong(line.nextToken()));
            return true;
        }catch (Exception e){
            System.err.println("SecondJobRecordReader "+ e.getMessage());
            return false;
        }

    }

    public FirstJobKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public FirstJobValue getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    public void close() throws IOException {
        reader.close();
    }
}
