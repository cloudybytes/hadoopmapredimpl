package com.cloudybytes.hadoopmapredimpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.tables.Table;

import java.util.ArrayList;

public class InputDriverJoin extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
        Configuration configuration = getConf();
        
        configuration.set("Separator.File1", ",");
        configuration.set("Separator.File2", ",");
        configuration.set("Name.File1", "rating");
        configuration.set("Name.File2", "users");
        configuration.set("Jointype","leftouter");
        configuration.set("Separator.Common", ",");
        Integer remidx1 = Table.getIdx(configuration.get("Name.File1"),"userid");
        Integer remidx2 = Table.getIdx(configuration.get("Name.File2"),"userid");
        Integer size1 = Table.getKeys(configuration.get("Name.File1")).size();
        Integer size2 = Table.getKeys(configuration.get("Name.File2")).size();
        configuration.set("Size.File1",size1.toString());
        configuration.set("Size.File2",size2.toString());
        configuration.set("JColumn1",remidx1.toString());
        configuration.set("JColumn2",remidx2.toString());
        Job job = new Job(configuration, "Multiple Input Example");
        MultipleInputs.addInputPath(job,new Path("rating.csv"), TextInputFormat.class, InputMapper1Join.class);
        MultipleInputs.addInputPath(job, new Path("users.csv"), TextInputFormat.class, InputMapper2Join.class);
        job.setJarByClass(InputDriverJoin.class);
        job.setReducerClass(InputReducerJoin.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
//        job.getConfiguration().set("mapreduce.output.textoutputformat.base_output_name", "result");
        TextOutputFormat.setOutputPath(job,new Path("Join"));
        
//        FileOutputFormat.setOutputPath(job, new Path("tempOutput"));
        job.waitForCompletion(true);

        return job.isSuccessful()? 0:-1;
    }
    public static void main(String [] args)
    {
        int result;
        //if(){//hasJoin
            try {
                result = ToolRunner.run(new Configuration(), new InputDriverJoin(), args);
                if (0 == result) {
                    System.out.println("Join completed Successfully...");
                    Configuration conf = new Configuration();
                    ArrayList<Pair<String,String>> a = Table.getKeys("rating");
                    ArrayList<Pair<String,String>> b = Table.getKeys("users");
                    ArrayList<String> cnames = new ArrayList<>();
                    ArrayList<String> ctypes = new ArrayList<>();
                    int idx1 = Table.getIdx("rating","userid");
                    int idx2 = Table.getIdx("users","userid");
                    cnames.add(a.get(idx1).getKey());
                    ctypes.add(a.get(idx1).getKey());
                    Integer i = 0;
                    for(Pair<String,String> x : a){
                        if(i != idx1){
                            cnames.add(x.getKey());
                            ctypes.add(x.getValue());
                        }
                        i++;
                    }
                    i = 0;
                    for(Pair<String,String> x : b){
                        if(i != idx2){
                            cnames.add(x.getKey());
                            ctypes.add(x.getValue());
                        }
                        i++;
                    }
                    conf.setStrings("cnames",cnames.toArray(new String[0]));
                    conf.setStrings("ctypes",ctypes.toArray(new String[0]));
                   result = ToolRunner.run(conf,new WhereDriver(),args);
                } else {
                    System.out.println("Job Failed...");
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            }
    }
}

