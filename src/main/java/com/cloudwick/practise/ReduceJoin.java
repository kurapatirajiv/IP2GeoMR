package com.cloudwick.practise;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Rajiv on 3/20/17.
 */
public class ReduceJoin {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length!=3){
            System.out.print("Not Enough arguments");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(ReduceJoin.class);
        job.setJobName("Multi Input Path Mapper Application");

        // Set input paths for two mapper functions
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, LocationMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, MessageMapper.class);

        // Set the Output file Path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Set the reducer code
        job.setReducerClass(IP2GeoReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
