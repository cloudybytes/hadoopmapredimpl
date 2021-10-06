package com.cloudybytes.hadoopmapredimpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONObject;
import utils.tables.Table;

import java.util.ArrayList;

public class InputDriverJoin extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
        JSONObject queryJSON = new JSONObject(args[0]);
        JSONArray joinJSON = queryJSON.getJSONArray("join");
        Configuration configuration = getConf();
        
        configuration.set("Separator.File1", ",");
        configuration.set("Separator.File2", ",");
        // TODO Add JSON Part
        // TODO done
        configuration.set("Name.File1", joinJSON.getString(1));
        configuration.set("Name.File2", joinJSON.getString(3));
        configuration.set("Jointype",joinJSON.getString(0));
        configuration.set("Separator.Common", ",");

        Integer remidx1 = Table.getIdx(configuration.get("Name.File1"),joinJSON.getString(2));
        Integer remidx2 = Table.getIdx(configuration.get("Name.File2"),joinJSON.getString(4));
        Integer size1 = Table.getKeys(configuration.get("Name.File1")).size();
        Integer size2 = Table.getKeys(configuration.get("Name.File2")).size();
        configuration.set("Size.File1",size1.toString());
        configuration.set("Size.File2",size2.toString());
        configuration.set("JColumn1",remidx1.toString());
        configuration.set("JColumn2",remidx2.toString());
        configuration.set("queryJSONString", args[0]);
        Job job = new Job(configuration, "Multiple Input Example");
        // TODO Add JSON Part
        // TODO done

        MultipleInputs.addInputPath(job,new Path(configuration.get("Name.File1") + ".csv"), TextInputFormat.class, InputMapper1Join.class);
        MultipleInputs.addInputPath(job, new Path(configuration.get("Name.File2") + ".csv"), TextInputFormat.class, InputMapper2Join.class);
        job.setJarByClass(InputDriverJoin.class);
        job.setReducerClass(InputReducerJoin.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextOutputFormat.setOutputPath(job,new Path("Join"));
        job.waitForCompletion(true);

        return job.isSuccessful()? 0:-1;
    }
    public static void main(String [] args) throws Exception {
        int result;
        // TODO Add JSON Part
        JSONObject queryJSON = new JSONObject(args[0]);
        boolean hasJoin = !queryJSON.isNull("join");
        if(hasJoin) {
            result = ToolRunner.run(new Configuration(), new InputDriverJoin(), args);
            if (0 == result) {
                System.out.println("Join completed Successfully...");
                Configuration conf = new Configuration();
                // TODO Add JSON Part
                ArrayList<Pair<String, String>> a = Table.getKeys("rating");
                ArrayList<Pair<String, String>> b = Table.getKeys("users");
                ArrayList<String> cNames = new ArrayList<>();
                ArrayList<String> cTypes = new ArrayList<>();
                // TODO Add JSON Part
                int idx1 = Table.getIdx("rating", "userid");
                int idx2 = Table.getIdx("users", "userid");
                cNames.add(a.get(idx1).getKey());
                cTypes.add(a.get(idx1).getKey());
                Integer i = 0;
                for (Pair<String, String> x : a) {
                    if (i != idx1) {
                        cNames.add(x.getKey());
                        cTypes.add(x.getValue());
                    }
                    i++;
                }
                i = 0;
                for (Pair<String, String> x : b) {
                    if (i != idx2) {
                        cNames.add(x.getKey());
                        cTypes.add(x.getValue());
                    }
                    i++;
                }
                conf.setStrings("cNames", cNames.toArray(new String[0]));
                conf.setStrings("cTypes", cTypes.toArray(new String[0]));
                result = ToolRunner.run(conf, new WhereDriver(), args);
                if (0 != result)
                    System.out.println("Job Failed...");
                else {
                    System.out.println("Job Completed Successfully");
                }
            }
        }
    }
}

