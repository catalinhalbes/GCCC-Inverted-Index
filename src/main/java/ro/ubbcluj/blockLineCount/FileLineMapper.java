package ro.ubbcluj.blockLineCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class FileLineMapper extends Mapper<Object, Text, FileBlockWritable, LongWritable> {
    private final FileBlockWritable fileBlock = new FileBlockWritable();
    private Configuration config;
    private long line = 0;
    private final LongWritable lineWritable = new LongWritable();

    @Override
    public void setup(Context context) {
        config = context.getConfiguration();
        String file = ((FileSplit)context.getInputSplit()).getPath().getName();
        long start = ((FileSplit)context.getInputSplit()).getStart();
        // divide by 0 probably intentional if dfs.blocksize fails
        long blockSize = config.getLong("dfs.blocksize", 0);
        fileBlock.setFile(file);
        fileBlock.setBlock(start / blockSize);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        line++;
        lineWritable.set(line);
        context.write(fileBlock, lineWritable);
    }
}