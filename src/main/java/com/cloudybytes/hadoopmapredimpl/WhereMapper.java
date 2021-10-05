package com.cloudybytes.hadoopmapredimpl;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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

    @Override
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        commonSeparator = configuration.get("Separator.Common");
        filename = configuration.get("Name.File");
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(commonSeparator);
        StringBuilder stringBuilder = new StringBuilder();
        // TODO JSon Implement
        // JSONArray whereJson = queryJSON.getJSONArray("where");
        Table row;
        // TODO Json Implement
        Boolean hasJoin = true;
        Boolean hasGroupBy = true;
        if (!hasJoin)
            row = new Table(values, Table.getKeys(filename));
        else {
            values[0] = values[0].trim();
            ArrayList<Pair<String, String>> keys = new ArrayList<>();
            String[] a = context.getConfiguration().getStrings("cNames");
            String[] b = context.getConfiguration().getStrings("cTypes");
            for (int i = 0; i < a.length; i++) {
                keys.add(Pair.of(a[i], b[i]));
            }
            row = new Table(values, keys);
        }
        // TODO Add given Where Conditions
        Boolean hasWhere = true;
        if(hasWhere) {
            // TODO Add given Where Conditions
            Pair<String, String> columnValue = row.getColumnValue("age");
            String compareOperator = ">=";
            String compareValue = "25";
            String type = columnValue.getValue();

            if (type.equalsIgnoreCase("Long")) {
                long toCompare = Long.parseLong(columnValue.getKey());
                long compareWith = Long.parseLong(compareValue);
                if (compareOperator.equalsIgnoreCase("=")) {
                    if (toCompare != compareWith)
                        return;
                } else if (compareOperator.equalsIgnoreCase(">")) {
                    if (toCompare <= compareWith)
                        return;
                } else if (compareOperator.equalsIgnoreCase("<")) {
                    if (toCompare >= compareWith)
                        return;
                } else if (compareOperator.equalsIgnoreCase(">=")) {
                    if (toCompare < compareWith)
                        return;
                } else if (compareOperator.equalsIgnoreCase("<=")) {
                    if (toCompare > compareWith)
                        return;
                } else if (compareOperator.equalsIgnoreCase("<>")) {
                    if (toCompare == compareWith)
                        return;
                }
            } else if (type.equalsIgnoreCase("Date")) {
                try {
                    Date toCompare = new SimpleDateFormat("dd-MMM-yy").parse(columnValue.getKey());
                    Date compareWith = new SimpleDateFormat("dd-MMM-yy").parse(compareValue);

                    if (compareOperator.equalsIgnoreCase("=")) {
                        if (toCompare != compareWith)
                            return;
                    } else if (compareOperator.equalsIgnoreCase(">")) {
                        if (!toCompare.after(compareWith))
                            return;
                    } else if (compareOperator.equalsIgnoreCase("<")) {
                        if (!toCompare.before(compareWith))
                            return;
                    } else if (compareOperator.equalsIgnoreCase(">=")) {
                        if (toCompare.before(compareWith))
                            return;
                    } else if (compareOperator.equalsIgnoreCase("<=")) {
                        if (toCompare.after(compareWith))
                            return;
                    } else if (compareOperator.equalsIgnoreCase("<>")) {
                        if (toCompare.equals(compareWith))
                            return;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
        // TODO Add Primary Key if GroupBy doesn't exist
        if(!hasGroupBy){
            // TODO Add given Select Conditions
                // TODO Integrate with JSON
            //		JSONArray havingJSON = queryJSON.getJSONArray("having_condition");
            //		String aggregateFunction = queryJSON.getJSONObject("aggr_function").toString();
            //		JSONArray columnsArray = queryJSON.getJSONArray("columns");
            ArrayList<String> columnsArray = new ArrayList<>();
            StringBuilder outputRow = new StringBuilder();
            for (int i = 0; i < columnsArray.size(); i++) {
                outputRow.append(",").append(row.getColumnValue(columnsArray.get(i)).getKey());
            }
            context.write(new Text(key.toString()), new Text(outputRow.toString()));
        }
        else {
            // TODO Implement JSON Integration
            Text groupByColumn = new Text(row.getColumnValue("age").getKey());
            context.write(groupByColumn, value);
        }
    }
}
