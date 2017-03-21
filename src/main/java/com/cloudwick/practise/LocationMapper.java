package com.cloudwick.practise;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Rajiv on 3/20/17.
 */
public class LocationMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String data[] = value.toString().split(",");
        context.write(new Text(data[0]), new Text("Location:"+data[1]));
    }
}
