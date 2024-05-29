package ro.ubbcluj.indexMinimize;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IndexMinimizeMapper extends Mapper<Object, Text, Text, Text> {
    private final Text word = new Text();
    private final Text fileAndRows = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(":");

        // TODO: check the length
        word.set(parts[0]);
        parts = parts[1].split("\t");

        // TODO: check the length here too...
        fileAndRows.set("(" + parts[0] + "," + parts[1] + ")");

        context.write(word, fileAndRows);
    }
}
