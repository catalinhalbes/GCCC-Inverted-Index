package ro.ubbcluj.indexMinimize;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexMinimizerReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder filesAndRows = new StringBuilder();
        for (Text text: values) {
            filesAndRows.append(text.toString());
        }
        result.set(filesAndRows.toString());
        context.write(key, result);
    }
}

