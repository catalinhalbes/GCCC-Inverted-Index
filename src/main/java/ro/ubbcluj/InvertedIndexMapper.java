package ro.ubbcluj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, LongWritable> {
    private Configuration config;
    private String fileName;
    private long line = 0;
    private final Map<String, Long> index = new HashMap<>();
    private final Set<String> stopWords = new HashSet<>();
    private final Text wordLocation = new Text();
    private final LongWritable lineWritable = new LongWritable();

    private void processIndexFile(String indexPath) {
        try {
            BufferedReader indexFile = new BufferedReader(new FileReader(indexPath));
            String line = indexFile.readLine();
            while (line != null) {
                String[] data = line.split("\t");
                String fileBlock = data[0];
                Long maxLinePerBlock = Long.valueOf(data[1]);
                index.put(fileBlock, maxLinePerBlock);
                line = indexFile.readLine();
            }
        } catch (IOException e) {
            System.err.println("Exception while reading index file: ");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void processStopWordsFile(String stopWordsPath) {
        try {
            BufferedReader stopWordsFile = new BufferedReader(new FileReader(stopWordsPath));
            String line = stopWordsFile.readLine();
            while (line != null) {
                stopWords.add(line.toLowerCase()); // TODO: Add flag to select case sensitiveness
                line = stopWordsFile.readLine();
            }
        } catch (IOException e) {
            System.err.println("Exception while reading stopwords file: ");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void computeStartLine(String fileName, long currentBlock) {
        // sum lines per previous blocks
        for (int i = 0; i < currentBlock; i++) {
            String fileBlock = fileName + "," + i;
            long linePerBlock = index.get(fileBlock);
            line += linePerBlock;
        }
    }

    private String getFilePathFromURI(URI uri) {
        // making a path out of a path seems redundant?
        Path path = new Path(uri.getPath());
        // so I thought until I ran into a FileNotFoundException
        // word count 2.0 example on hadoop tutorial uses this
        // assuming it is some odd way to indirect to cache
        return path.getName();
    }

    @Override
    public void setup(Context context) throws IOException {
        config = context.getConfiguration();

        // load cached files
        URI[] fileURIs = Job.getInstance(config).getCacheFiles();

        String indexPath = getFilePathFromURI(fileURIs[0]);
        processIndexFile(indexPath);

        String stopWordsPath = getFilePathFromURI(fileURIs[1]);
        processStopWordsFile(stopWordsPath);

        fileName = ((FileSplit)context.getInputSplit()).getPath().getName();

        long start = ((FileSplit)context.getInputSplit()).getStart();
        // divide by 0 probably intentional if dfs.blocksize fails
        long blockSize = config.getLong("dfs.blocksize", 0);
        long currentBlock = start / blockSize;

        computeStartLine(fileName, currentBlock);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        line++; // at beginning for indexing from 1

        StringTokenizer itr = new StringTokenizer(value.toString(), "\"',.()[]?!#$%^&*+=_- \t\\/<>|{}:;~`");

        String currentToken;
        String newKey;
        while (itr.hasMoreTokens()) {
            currentToken = itr.nextToken().toLowerCase(); // TODO: Add flag to select case sensitiveness
            if (!stopWords.contains(currentToken)) {
                newKey = currentToken + "," + fileName;
                wordLocation.set(newKey);
                lineWritable.set(line);
                context.write(wordLocation, lineWritable);
            }
        }
    }
}