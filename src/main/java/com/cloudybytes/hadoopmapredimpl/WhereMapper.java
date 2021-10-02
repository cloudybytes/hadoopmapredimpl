 package com.cloudybytes.hadoopmapredimpl;

 import org.apache.commons.lang3.tuple.Pair;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;
 import org.json.JSONArray;
 import org.json.JSONObject;
 import utils.tables.Table;

 import java.io.IOException;

 public class WhereMapper extends Mapper<LongWritable, Text, Text, Text> {
     private static String commonSeparator;
     private static JSONObject queryJSON;
     private static String filename;
     public void setup(Context context){
         Configuration configuration = context.getConfiguration();
         commonSeparator = configuration.get("Separator.Common");
         filename = configuration.get("Name.File");
     }

     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String[] values = value.toString().split(commonSeparator);
         StringBuilder stringBuilder = new StringBuilder();

         JSONArray whereJson = queryJSON.getJSONArray("where");

         Table row = new Table(values, Table.getKeys(filename));
         Pair<String,String> columnValue = row.getColumnValue(whereJson.get(0).toString());
         String compareOperator = whereJson.get(1).toString();
         String compareValue = whereJson.get(2).toString();
         String type = columnValue.getValue();

         if(type.equalsIgnoreCase("Long")){
             Long tocompare = Long.parseLong(columnValue.getKey());
             Long comparewith = Long.parseLong(compareValue);
             if(compareOperator.equalsIgnoreCase("=")){
                 if(tocompare != comparewith)
                     return;
             }
             else if(compareOperator.equalsIgnoreCase(">")){
                 if(tocompare <= comparewith)
                 return;
             }
             else if(compareOperator.equalsIgnoreCase("<")){
                 if(tocompare >= comparewith)
                 return;
             }
             else if(compareOperator.equalsIgnoreCase(">=")){
                 if(tocompare < comparewith)
                 return;
             }
             else if(compareOperator.equalsIgnoreCase("<=")){
                 if(tocompare > comparewith)
                 return;
             }
             else if(compareOperator.equalsIgnoreCase("<>")){
                 if(tocompare == comparewith)
                     return;
             }
         }
         else if(type.equalsIgnoreCase("Date")){
             if(compareOperator.equalsIgnoreCase("=")){
//                 if(columnValue != compareValue)
                     return;
             }
             else if(compareOperator.equalsIgnoreCase(">")){
                 //if(columnValue <= compareValue)
                 return;
             }
             else if(compareOperator.equalsIgnoreCase("<")){
                 //if(columnValue >= compareValue)
                 return;
             }
             else if(compareOperator.equalsIgnoreCase(">=")){
                 //if(columnValue < compareValue)
                 return;
             }
             else if(compareOperator.equalsIgnoreCase("<=")){
                 //if(columnValue > compareValue)
                 return;
             }
             else if(compareOperator.equalsIgnoreCase("<>")){
                 if(columnValue == compareValue)
                     return;
             }
         }





         Text groupByColumn = new Text(queryJSON.get("group_by_column").toString());
         context.write(groupByColumn, value);
     }
 }
