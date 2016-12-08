import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Tifani on 11/30/2016.
 */
public class TwitterRank {
    public static final int RANK = 5;
    private static final int itr = 3;

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        // Job 1: Initialize
        TwitterRank.initializeTwitterRank(inputPath, outputPath + "/iterasi0");

        // Job 2: Calculate page rank
        for (int i=0; i < itr ; i++) {
            //Job 2: Calculate new rank
            int nextI = i + 1;
            TwitterRank.runRankCalculation(outputPath + "/iterasi" + i, outputPath + "/iterasi" + nextI, nextI);
        }

        // Job 3: Order by rank
        TwitterRank.runRankOrdering(outputPath + "/iterasi" + itr, outputPath + "/result");
    }

    private static void initializeTwitterRank(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "[tifani] #1 initialize-twitter-rank");

        // Set Classes
        job.setJarByClass(TwitterRank.class);
        job.setMapperClass(InitMapper.class);
        job.setReducerClass(InitReducer.class);
        job.setNumReduceTasks(122);

        // Set Output and Input Parameters
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }

    private static void runRankCalculation(String inputPath, String outputPath, int itr) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "[tifani] #2 run-calculation (itr-" + itr +  ")");

        // Set Classes
        job.setJarByClass(TwitterRank.class);
        job.setMapperClass(RankCalcMapper.class);
        job.setReducerClass(RankCalcReducer.class);
        job.setNumReduceTasks(122);

        // Set Output and Input Parameters
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }

    private static void runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "[tifani] #3 rank-ordering");

        // Set Classes
        job.setJarByClass(TwitterRank.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        job.setNumReduceTasks(122);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
