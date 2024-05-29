package ro.ubbcluj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ro.ubbcluj.blockLineCount.FileBlockWritable;
import ro.ubbcluj.blockLineCount.FileLineMapper;
import ro.ubbcluj.blockLineCount.MaxLineReducer;
import ro.ubbcluj.indexMinimize.IndexMinimizeMapper;
import ro.ubbcluj.indexMinimize.IndexMinimizerReducer;
import ro.ubbcluj.wordIndex.InvertedIndexMapper;
import ro.ubbcluj.wordIndex.InvertedIndexReducer;

import java.io.*;

public class Main {
    private static final String BLOCK_LINES_TMP_SUB_FOLDER = "/block-lines";
    private static final String INDEX_TMP_SUB_FOLDER = "/index";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 4) {
            System.out.println("args: input, temp, output, stopwords");
            System.out.println("input - input folder");
            System.out.println("temp - intermediary results folder");
            System.out.println("output - output folder");
            System.out.println("stopwords - stopwords file, txt, line separated");
            System.exit(1);
        }

        final String inputFolder = args[0];
        final String tempFolder = args[1];
        final String outFolder = args[2];
        final String stopWordsFile = args[3];

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "line rebuild");
        job1.setJarByClass(Main.class);

        job1.setMapperClass(FileLineMapper.class);
        job1.setCombinerClass(MaxLineReducer.class);
        //job1.setReducerClass(IntSumReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);

        job1.setOutputKeyClass(FileBlockWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job1, new Path(tempFolder + BLOCK_LINES_TMP_SUB_FOLDER));

        boolean ok = job1.waitForCompletion(true);

        if (!ok) {
            System.out.println("Block line numbering job failed");
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "inverted index");
        job2.setJarByClass(Main.class);

        job2.setMapperClass(InvertedIndexMapper.class);
        job2.setReducerClass(InvertedIndexReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job2, new Path(tempFolder + INDEX_TMP_SUB_FOLDER));

        // TODO: read into how to do a proper output
        job2.addCacheFile(new Path(tempFolder + BLOCK_LINES_TMP_SUB_FOLDER + "/part-r-00000").toUri());
        job2.addCacheFile(new Path(stopWordsFile).toUri());

        ok = job2.waitForCompletion(true);

        if (!ok) {
            System.out.println("Word Line indexing job failed");
            System.exit(1);
        }

        Job job3 = Job.getInstance(conf, "inverted index minimize");
        job3.setJarByClass(Main.class);

        job3.setMapperClass(IndexMinimizeMapper.class);
        job3.setReducerClass(IndexMinimizerReducer.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(tempFolder + INDEX_TMP_SUB_FOLDER));
        FileOutputFormat.setOutputPath(job3, new Path(outFolder));

        ok = job3.waitForCompletion(true);

        System.exit(ok ? 0 : 1);
    }
}