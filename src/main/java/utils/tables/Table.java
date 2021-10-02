package utils.tables;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;


public class Table {
    ArrayList<Pair<String,String>> row;
    public Table(String[] vals,ArrayList<String> keys){
        row = new ArrayList<>();
        int i = 0;
        for (String key:keys) {
            row.add(Pair.of(key,vals[i]));
            i++;
        }
    }
    public static ArrayList<String> getKeys(String tableName){
        ArrayList<String> a = new ArrayList<>();
        if(tableName.equalsIgnoreCase("rating")){
            a.add("userid");
            a.add("movieid");
            a.add("rating");
            a.add("timestamp");
        }
        else if(tableName.equalsIgnoreCase("movies")){
            a.add("movieid");
            a.add("title");
            a.add("releasedate");
            a.add("unknown");
            a.add("action");
            a.add("adventure");
            a.add("animation");
            a.add("children");
            a.add("comedy");
            a.add("crime");
            a.add("documentary");
            a.add("drama");
            a.add("fantasy");
            a.add("film_noir");
            a.add("horror");
            a.add("musical");
            a.add("mystery");
            a.add("romance");
            a.add("sci_fi");
            a.add("thriller");
            a.add("war");
            a.add("western");
        }
        else if(tableName.equalsIgnoreCase("zipcodes")){
            a.add("zipcode");
            a.add("zipcodetype");
            a.add("city");
            a.add("state");
        }
        else if(tableName.equalsIgnoreCase("users")){
            a.add("userid");
            a.add("age");
            a.add("gender");
            a.add("occupation");
            a.add("zipcode");
        }
        return a;
    }
    public static int getIdx(String filename,String columnname){
        int i = 0;
        for(String x : Table.getKeys(filename)){
            if(x.equalsIgnoreCase(columnname)){
                return i;
            }
            i++;
        }
        return -1;
    }
    Object getColumnValue(String columnName) {
        for (Pair<String,String> x: row) {
            if(x.getKey().equalsIgnoreCase(columnName))
                return x.getValue();
        }
        return null;
    }

    String groupByString(String[] columns) {
        return null;
    }

    Boolean checkColumnValue(String columnName, String value) {
        for (Pair<String,String> x: row) {
            if(x.getKey().equalsIgnoreCase(columnName) && x.getValue().equalsIgnoreCase(value))
                return true;
        }
        return false;
    }

    Object getAggregate(String operation, String column, ArrayList<Table> arr) {
        return null;
    }

    Boolean compareAggregate(String column, String operation, String comparisonOperator, String value, ArrayList<Table> arr) {
        return null;
    }
}
