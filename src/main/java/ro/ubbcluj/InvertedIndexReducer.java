package ro.ubbcluj;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, LongWritable, Text, Text> {
    private final Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<LongWritable> lines, Context context) throws IOException, InterruptedException {
            /*long maxLine = 0;
            for (LongWritable line: lines) {
                long value = line.get();
                if (value > maxLine)
                    maxLine = value;
            }
            result.set(maxLine);
            context.write(key, result);*/

        StringBuilder acc = new StringBuilder();
        for (LongWritable line : lines) {
            long value = line.get();
            if (acc.length() == 0) {
                acc = new StringBuilder(String.valueOf(value));
            } else {
                acc.append(",").append(value);
            }
        }
        result.set(acc.toString());
        context.write(key, result);
    }
}
