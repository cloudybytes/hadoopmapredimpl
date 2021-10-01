package com.cloudybytes.hadoopmapredimpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.json.JSONObject;

public class JoinMapRed {
    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String sqlQuery = args[2];
////        JSONObject queryJSON = parseSQL(sqlQuery.toLowerCase());
////        System.out.println(queryJSON);
//        conf.set("queryJSONString", queryJSON.toString());
//
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(SQLExecutor.class);
//        job.setMapperClass(QueryMapper.class);
//        job.setReducerClass(QueryReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0].concat("/").concat(queryJSON.getString("table")).concat(".csv")));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));1
//
//        long start = new Date().getTime();
//        boolean status = job.waitForCompletion(true);
//        long end = new Date().getTime();
//        System.out.println("Hadoop Execution Time "+(end-start) + "milliseconds");
//        System.out.println("Mapper input -> <LongWritable, Text> (offset, line of file)");
//        System.out.println("Mapper output -> <Text, Text> (columns in group by separated by _, row satisfying where condition)");
//        System.out.println("Reducer input -> <Text, Text> (columns in group by separated by _, row)");
//        System.out.println("Reducer output -> <Text, Text> (columns in group by separated by _, row containing required columns and satisfying having condition)");
//
//
////    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
