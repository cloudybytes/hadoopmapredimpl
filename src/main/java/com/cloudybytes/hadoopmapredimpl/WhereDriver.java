package com.cloudybytes.hadoopmapredimpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WhereDriver  extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
        Configuration configuration = getConf();
//        String[] cNames = configuration.getStrings("cNames");
//        String[] cTypes = configuration.getStrings("cTypes");
        configuration.set("Separator.File", ",");
        configuration.set("Name.File", "users");
        configuration.set("Separator.Common", ",");
        Job job = new Job(configuration, "Where Example");
        FileInputFormat.addInputPath(job,new Path("Join/part-r-00000"));
        job.setJarByClass(WhereDriver.class);
        job.setReducerClass(WhereReducer.class);
        job.setMapperClass(WhereMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, new Path("whereOutput"));
        job.waitForCompletion(true);

        if(job.isSuccessful()){
            configuration.set("","");
        }

        return job.isSuccessful()? 0:-1;
    }
    public static void main(String [] args)
    {
        int result;
        //if(){//hasJoin
        try {
            result = ToolRunner.run(new Configuration(), new WhereDriver(), args);
            if (0 == result) {
                System.out.println("Job completed Successfully...");
            } else {
                System.out.println("Job Failed...");
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        //}
        //else{ //no Join

        //}
    }
}

