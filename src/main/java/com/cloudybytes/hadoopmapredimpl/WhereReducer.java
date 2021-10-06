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
		if (arr.size() == 0)
			return;
		// TODO Integrate with JSON
		JSONArray havingJSON = queryJSON.getJSONArray("having_condition");
		String aggregateFunction = queryJSON.getJSONObject("aggr_function").toString();
		JSONArray columnsJSONArray = queryJSON.getJSONArray("select_columns");
		ArrayList<String> columnsArray = new ArrayList<String>();
		for(i = 0; i < columnsJSONArray.length(); i++){
			columnsArray.add(columnsJSONArray.getString(i));
		}
		// TODO add GroupBy Column
		columnsArray.add(queryJSON.getString("group_by_column"));

		StringBuilder outputRow = new StringBuilder();

		String aggr = String.valueOf(arr.get(0).getAggregate(aggregateFunction, havingJSON.getString(0), arr));
		if (!arr.get(0).compareAggregate("age"/*havingJSON.get(0).toString()*/, "count"/*aggregateFunction*/, ">"/*havingJSON.get(1).toString()*/, "10"/*havingJSON.get(2).toString()*/, arr)) {
			return;
		}
		for (i = 0; i < columnsArray.size(); i++) {
			outputRow.append(",").append(arr.get(0).getColumnValue(columnsArray.get(i)).getKey());
		}
		outputRow.append(",").append(aggr);
		context.write(key, new Text(outputRow.toString()));
	}
}
