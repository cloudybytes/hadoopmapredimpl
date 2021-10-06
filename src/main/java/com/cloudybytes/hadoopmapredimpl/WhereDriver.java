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
import org.json.JSONObject;

public class WhereDriver  extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
        JSONObject queryJSON = new JSONObject(args[0]);
        Configuration configuration = getConf();
        configuration.set("Separator.File", ",");
        // TODO Add JSON Part
        configuration.set("Name.File", queryJSON.getString("from_table"));
        configuration.set("Separator.Common", ",");
        Job job = new Job(configuration, "Where Example");
        // TODO Add JSON Part
        boolean hasJoin = !queryJSON.isNull("join");

        if(hasJoin)
            FileInputFormat.addInputPath(job,new Path("Join/part-r-00000"));
        else{
//           TODO add table from JSON
            FileInputFormat.addInputPath(job,new Path(""));
        }
        job.setJarByClass(WhereDriver.class);
        job.setReducerClass(WhereReducer.class);
        job.setMapperClass(WhereMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // TODO Add JSON Part
        boolean hasGroupBy = !queryJSON.isNull("group_by_column");
        if(!hasGroupBy)
            job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, new Path("whereOutput"));
        job.waitForCompletion(true);
        return job.isSuccessful()? 0:-1;
    }
    public static void main(String [] args)
    {
        int result;
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
    }
}

