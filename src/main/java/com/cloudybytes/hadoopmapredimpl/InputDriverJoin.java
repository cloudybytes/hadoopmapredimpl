package com.cloudybytes.hadoopmapredimpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class InputDriverJoin  extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
        Configuration configuration = getConf();
        configuration.set("Separator.File1", ",");
        configuration.set("Separator.File2", ",");
        configuration.set("Name.File1", "users");
        configuration.set("Name.File2", "zipcodes");
        configuration.set("Jointype","leftouter");
        configuration.set("Separator.Common", ";");
        Job job = new Job(configuration, "Multiple Input Example");
        MultipleInputs.addInputPath(job,new Path("users.csv"), TextInputFormat.class, InputMapper1Join.class);
        MultipleInputs.addInputPath(job, new Path("zipcodes.csv"), TextInputFormat.class, InputMapper2Join.class);
        job.setJarByClass(InputDriverJoin.class);
        job.setReducerClass(InputReducerJoin.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path("output"));
        job.waitForCompletion(true);
        return job.isSuccessful()? 0:-1;
    }
    public static void main(String [] args)
    {
        int result;
        try{
            result= ToolRunner.run(new Configuration(), new InputDriverJoin(), args);
            if(0 == result)
            {
                System.out.println("Job completed Successfully...");
            }
            else
            {
                System.out.println("Job Failed...");
            }
        }
        catch(Exception exception)
        {
            exception.printStackTrace();
        }
    }
}

