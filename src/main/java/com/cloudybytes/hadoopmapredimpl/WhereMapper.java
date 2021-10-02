 package com.cloudybytes.hadoopmapredimpl;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;
 import utils.tables.Table;

 public class WhereMapper extends Mapper<LongWritable, Text, Text, Text> {
     private static String commonSeparator;
     public void setup(Context context){
         Configuration configuration = context.getConfiguration();
         commonSeparator = configuration.get("Separator.Common");
     }

     public void map(LongWritable key, Text value, Context context){
         String[] values = value.toString().split(commonSeparator);
         StringBuilder stringBuilder = new StringBuilder();


//         int remidx = 0;
//         for(int index=0; index<values.length; index++)
//         {
//             if(index != remidx)
//                 stringBuilder.append(values[index]+commonSeparator);
//         }
//         if(values[remidx] != null && !"NULL".equalsIgnoreCase(values[remidx]))
//         {
//             context.write(new Text(values[remidx]), new Text(commonSeparator+stringBuilder.toString()));
//         }
     }
 }
