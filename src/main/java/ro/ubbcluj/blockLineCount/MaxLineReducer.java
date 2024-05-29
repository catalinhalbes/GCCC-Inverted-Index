package ro.ubbcluj.blockLineCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxLineReducer extends Reducer<FileBlockWritable, LongWritable, FileBlockWritable, LongWritable> {
    private final LongWritable result = new LongWritable();

    @Override
    public void reduce(FileBlockWritable key, Iterable<LongWritable> lines, Context context) throws IOException, InterruptedException {
        long maxLine = 0;
        for (LongWritable line: lines) {
            long value = line.get();
            if (value > maxLine)
                maxLine = value;
        }
        result.set(maxLine);
        context.write(key, result);
    }
}