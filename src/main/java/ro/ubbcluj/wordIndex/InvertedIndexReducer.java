package ro.ubbcluj.wordIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class InvertedIndexReducer extends Reducer<Text, LongWritable, Text, Text> {
    private final Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<LongWritable> lines, Context context) throws IOException, InterruptedException {
        StringBuilder acc = new StringBuilder();

        ArrayList<LongWritable> linesSorted = new ArrayList<LongWritable>();
        lines.forEach(linesSorted::add);

        linesSorted.sort(
                (a, b) -> Math.toIntExact(b.get() - a.get())
        );

        for (LongWritable line : linesSorted) {
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
