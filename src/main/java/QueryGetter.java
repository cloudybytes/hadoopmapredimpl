import org.apache.commons.httpclient.HttpClient;
import org.codehaus.jettison.json.JSONObject;

import org.apache.http;

public class QueryGetter {
    JSONObject query;
    JSONObject  result;
    void getParsedQuery(String sqlQuery){
        query = new JSONObject();
        query.put("user_sql", sqlQuery);    

    HttpClient httpClient = HttpClientBuilder.create().build();

try {
    HttpPost request = new HttpPost("http://127.0.0.1:8000/user_sql");
    StringEntity params = new StringEntity(query.toString());
    request.addHeader("content-type", "application/json");
    request.setEntity(params);
    httpClient.execute(request);
// handle response here...
} catch (Exception ex) {
    // handle exception here
} finally {
    httpClient.close();
}
    }
}
