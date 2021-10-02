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
 import java.text.ParseException;
 import java.text.SimpleDateFormat;
 import java.util.ArrayList;
 import java.util.Date;

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
        //TODO JSon Implement
        //  JSONArray whereJson = queryJSON.getJSONArray("where");
         Table row;
         Boolean hasJoin = true;
         if(!hasJoin)
            row = new Table(values, Table.getKeys(filename));
         else{
             values[0] = values[0].trim();
             ArrayList<Pair<String,String>> keys = new ArrayList<>();
             String[] a = context.getConfiguration().getStrings("cnames");
             String[] b = context.getConfiguration().getStrings("ctypes");
             for(int i = 0;i<a.length;i++){
                 keys.add(Pair.of(a[i],b[i]));
             }
             row = new Table(values,keys);
         }

         Pair<String,String> columnValue = row.getColumnValue("age");
         String compareOperator = ">=";
         String compareValue = "25";
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
             try {
                 Date tocompare = new SimpleDateFormat("dd-MMM-yy").parse(columnValue.getKey());
                 Date comparewith = new SimpleDateFormat("dd-MMM-yy").parse(compareValue);

                 if(compareOperator.equalsIgnoreCase("=")){
                     if(tocompare != comparewith)
                         return;
                 }
                 else if(compareOperator.equalsIgnoreCase(">")){
                     if(!tocompare.after(comparewith))
                         return;
                 }
                 else if(compareOperator.equalsIgnoreCase("<")){
                     if(!tocompare.before(comparewith))
                         return;
                 }
                 else if(compareOperator.equalsIgnoreCase(">=")){
                     if(tocompare.before(comparewith))
                         return;
                 }
                 else if(compareOperator.equalsIgnoreCase("<=")){
                     if(tocompare.after(comparewith))
                         return;
                 }
                 else if(compareOperator.equalsIgnoreCase("<>")){
                     if(tocompare.equals(comparewith))
                         return;
                 }
             }
             catch (ParseException e) {
                 e.printStackTrace();
             }
         }


         Text groupByColumn = new Text(row.getColumnValue("userid").getKey());
         context.write(groupByColumn, value);
     }
 }
