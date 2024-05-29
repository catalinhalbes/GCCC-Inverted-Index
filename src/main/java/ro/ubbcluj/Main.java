package ro.ubbcluj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 4) {
            System.out.println("args: input, temp, output, stopwords");
            System.out.println("input - input folder");
            System.out.println("temp - intermediary results folder");
            System.out.println("output - output folder");
            System.out.println("stopwords - stopwords file, txt, line separated");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "line rebuild");
        job1.setJarByClass(Main.class);

        job1.setMapperClass(FileLineMapper.class);
        job1.setCombinerClass(MaxLineReducer.class);
        //job1.setReducerClass(IntSumReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);

        job1.setOutputKeyClass(FileBlockWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean ok = job1.waitForCompletion(true);

        if (!ok) {
            System.out.println("Stopping early because of job fail");
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "inverted index");
        job2.setJarByClass(Main.class);

        job2.setMapperClass(InvertedIndexMapper.class);
        job2.setReducerClass(InvertedIndexReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // TODO: read into how to do a proper output
        job2.addCacheFile(new Path(args[1] + "/part-r-00000").toUri());
        job2.addCacheFile(new Path(args[3]).toUri());

        ok = job2.waitForCompletion(true);

        System.exit(ok ? 0 : 1);
    }
}