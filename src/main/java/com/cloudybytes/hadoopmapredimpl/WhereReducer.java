package com.cloudybytes.hadoopmapredimpl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import utils.tables.Table;
import utils.tables.TableFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;

public class WhereReducer extends Reducer<Text, Text, Text, Text> {
	private static JSONObject queryJSON;
	private static Configuration configuration;

	public void setup(Context context){
		configuration = context.getConfiguration();
		queryJSON = new JSONObject(configuration.get("queryJSONString"));
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<Table> arr = new ArrayList<Table>();
		String[] cNames = configuration.getStrings("cNames");
        String[] cTypes = configuration.getStrings("cTypes");
		ArrayList<Pair<String, String>> keys = new ArrayList<Pair<String, String>>();
		int i;

		for(i = 0; i < cNames.length; i++){
			keys.add(Pair.of(cNames[i], cTypes[i]));
		}

		for (Text v : values) {
			Table row = TableFactory.getTable(v.toString(), keys);
			if (row == null)
				continue;
			arr.add(row);
		}
		if (arr.size() == 0)
			return;

		JSONArray havingJSON = queryJSON.getJSONArray("having_condition");
		String aggregateFunction = queryJSON.getJSONObject("aggr_function").toString();
		JSONArray columnsArray = queryJSON.getJSONArray("columns");
		StringBuilder outputRow = new StringBuilder();

		String aggr = String.valueOf(arr.get(0).getAggregate(aggregateFunction, havingJSON.get(0).toString(), arr));
		if (!arr.get(0).compareAggregate(havingJSON.get(0).toString(), aggregateFunction, havingJSON.get(1).toString(), havingJSON.get(2).toString(), arr)) {
			return;
		}
		for (i = 0; i < columnsArray.length(); i++) {
			outputRow.append(arr.get(0).getColumnValue(columnsArray.getString(i)).toString()).append(",");
		}
		outputRow.append(aggr);
		context.write(key, new Text(outputRow.toString()));
	}
}
