package com.cloudwick.practise;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Rajiv on 3/20/17.
 */
public class IP2GeoReducer extends Reducer<Text,Text,Text,Text> {


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {

        ArrayList<String> col1 = new ArrayList<String>();
        String col2 ="N/A";
        // List contains both Messages and Location
        for(Text t: values){
            String value = t.toString();
            String data[] = value.toString().split(":");
            if(value.contains("Message")) {
                col1.add(data[1]);
            }
            if(value.contains("Location")){
                 col2 = data[1];
            }
        }
        // Write the messages for each key
        for(String s: col1){
            context.write(new Text(s), new Text("," + col2));
        }
    }
}
