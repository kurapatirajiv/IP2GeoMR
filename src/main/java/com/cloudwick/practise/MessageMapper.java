package com.cloudwick.practise;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Rajiv on 3/20/17.
 */
public class MessageMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    public void map(LongWritable key ,Text value,Context context)  throws IOException, InterruptedException{

        String data[] = value.toString().split(",");
        context.write(new Text(data[2]), new Text("Message:"+data[0]+","+data[1]));
    }

}
