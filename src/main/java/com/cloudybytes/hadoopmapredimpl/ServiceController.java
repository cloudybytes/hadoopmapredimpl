package com.cloudybytes.hadoopmapredimpl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;
import utils.tables.Table;

@RestController
public class ServiceController {
	@GetMapping(value = "/hello")
	public String hello(){
		return "Hello";
	}

	@PostMapping(value = "/execute", consumes = "application/json")
	public Map<String, Object> execute(@RequestBody String queryString) throws Exception {
		String[] columns;
		int result;
		String[] args = new String[1];
		JSONObject queryJSON = new JSONObject(queryString);
		args[0] = queryString;
		boolean hasJoin = !queryJSON.isNull("join");

		long start = new Date().getTime();
		
		if(hasJoin) {
			result = ToolRunner.run(new Configuration(), new InputDriverJoin(), args);
			if (0 == result) {
				System.out.println("Join completed Successfully...");
				Configuration conf = new Configuration();
				JSONArray joinJSON = queryJSON.getJSONArray("join");
				// TODO Add JSON Part
				ArrayList<Pair<String, String>> a = Table.getKeys(joinJSON.getString(1));
				ArrayList<Pair<String, String>> b = Table.getKeys(joinJSON.getString(3));
				ArrayList<String> cNames = new ArrayList<>();
				ArrayList<String> cTypes = new ArrayList<>();
				// TODO Add JSON Part
				int idx1 = Table.getIdx(joinJSON.getString(1), joinJSON.getString(2));
				int idx2 = Table.getIdx(joinJSON.getString(3), joinJSON.getString(4));
				cNames.add(a.get(idx1).getKey());
				cTypes.add(a.get(idx1).getKey());
				int i = 0;
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

			}
		}
		else {
			Configuration conf = new Configuration();
			result = ToolRunner.run(conf, new WhereDriver(), args);
		}
		if (0 != result)
			System.out.println("Job Failed...");
		else {
			System.out.println("Job Completed Successfully");
			String filename;
			if(queryJSON.isNull("group_by_column")){
				filename = "whereOutput/part-m-00000";
			}
			else{
				filename = "whereOutput/part-r-00000";
			}
			int i=0;
			Scanner subFile = new Scanner(new File(filename));
			File myFile = new File("./src/main/resources/static/output.csv");
			BufferedWriter writer = new BufferedWriter(new FileWriter(myFile, false));
			while(subFile.hasNext())
			{
				String line = subFile.nextLine();
				line.trim().replaceAll(" +", ""); //check for double spaces
				writer.append(line);
				writer.append('\n');
			}
			writer.close();
		}
		long end = new Date().getTime();
		FileUtils.deleteDirectory(new File("whereOutput"));
		if(hasJoin)
			FileUtils.deleteDirectory(new File("Join"));
		Map<String, Object> response = new HashMap<>();
		response.put("time", (end - start) + " milliseconds");
		response.put("output_url","/output.csv");
		return response;
	}
}
