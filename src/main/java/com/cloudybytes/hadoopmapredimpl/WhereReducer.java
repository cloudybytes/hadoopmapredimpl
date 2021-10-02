package com.cloudybytes.hadoopmapredimpl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;

public class WhereReducer extends Reducer<Text, Text, Text, Text> {
    private static JSONObject queryJSON;

    public void setup(Context context){
        Configuration configuration = context.getConfiguration();
        queryJSON = new JSONObject(configuration.get("queryJSONString"));
    }

    public void reduce(Text key, Iterable<Text> values, Context context){

    }
}
