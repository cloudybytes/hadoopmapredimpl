package com.cloudybytes;


import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class QueryGetter {

    public String getParsedQuery(String sqlQuery){
//        URL url = new URL("https://127.0.0.1:8000/user_sql/");
//        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//
//        connection.setRequestMethod("POST");
//        Map<String, String> params = new HashMap<>();
//        params.put("user_sql", sqlQuery);
//
//        StringBuilder postData = new StringBuilder();
//        for (Map.Entry<String, String> param : params.entrySet()) {
//            if (postData.length() != 0) {
//                postData.append('&');
//            }
//            postData.append(URLEncoder.encode(param.getKey(), "UTF-8"));
//            postData.append('=');
//            postData.append(URLEncoder.encode(String.valueOf(param.getValue()), "UTF-8"));
//        }
//
//        byte[] postDataBytes = postData.toString().getBytes("UTF-8");
//        connection.setDoOutput(true);
//        try (DataOutputStream writer = new DataOutputStream(connection.getOutputStream())) {
//            writer.write(postDataBytes);
//
//            StringBuilder content;
//
//            try (BufferedReader in = new BufferedReader(
//                    new InputStreamReader(connection.getInputStream()))) {
//                String line;
//                content = new StringBuilder();
//                while ((line = in.readLine()) != null) {
//                    content.append(line);
//                    content.append(System.lineSeparator());
//                }
//            }
//            System.out.println(content);
//        } finally {
//            connection.disconnect();
//        }
        return "";
    }
}
