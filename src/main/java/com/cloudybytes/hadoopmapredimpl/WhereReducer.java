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
		ArrayList<Table> arr = new ArrayList<>();
		String[] cNames = configuration.getStrings("cNames");
        String[] cTypes = configuration.getStrings("cTypes");
		ArrayList<Pair<String, String>> keys = new ArrayList<>();
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
		if (arr.isEmpty())
			return;
		// TODO Integrate with JSON
		JSONArray havingJSON = queryJSON.getJSONArray("having_condition");
		String aggregateFunction = queryJSON.getString("aggr_function");
		JSONArray columnsJSONArray = queryJSON.getJSONArray("select_columns");
		ArrayList<String> columnsArray = new ArrayList<>();
		for(i = 0; i < columnsJSONArray.length(); i++){
			columnsArray.add(columnsJSONArray.getString(i));
		}
		
		StringBuilder outputRow = new StringBuilder();

		String aggr = String.valueOf(arr.get(0).getAggregate(aggregateFunction, havingJSON.getString(0), arr));
		if (!arr.get(0).compareAggregate(havingJSON.getString(0), aggregateFunction, havingJSON.getString(1), havingJSON.getString(2), arr)) {
			return;
		}
		for (i = 0; i < columnsArray.size(); i++) {
			if(arr.get(0).getColumnValue(columnsArray.get(i)) != null){
				outputRow.append(arr.get(0).getColumnValue(columnsArray.get(i)).getKey()).append(", ");
			}
		}
		outputRow.append(aggr);
		context.write(new Text(""), new Text(outputRow.toString()));
	}
}
