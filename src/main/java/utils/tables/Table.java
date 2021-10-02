package utils.tables;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;


public class Table {
    ArrayList<Pair<Pair<String,String>,String>> row;
    public Table(String[] vals,ArrayList<Pair<String,String>> keys){
        row = new ArrayList<>();
        int i = 0;
        for (Pair<String,String> key:keys) {
            row.add(Pair.of(key,vals[i]));
            i++;
        }
    }
    public static ArrayList<Pair<String,String>> getKeys(String tableName){
        ArrayList<Pair<String,String>> a = new ArrayList<>();
        if(tableName.equalsIgnoreCase("rating")){
            a.add(Pair.of("userid","Integer"));
            a.add(Pair.of("movieid","Integer"));
            a.add(Pair.of("rating","Integer"));
            a.add(Pair.of("timestamp","Long"));
        }
        else if(tableName.equalsIgnoreCase("movies")){
            a.add(Pair.of("movieid","Integer"));
            a.add(Pair.of("title","Integer"));
            a.add(Pair.of("releasedate","Date"));
            a.add(Pair.of("unknown","Integer"));
            a.add(Pair.of("action","Integer"));
            a.add(Pair.of("adventure","Integer"));
            a.add(Pair.of("animation","Integer"));
            a.add(Pair.of("children","Integer"));
            a.add(Pair.of("comedy","Integer"));
            a.add(Pair.of("crime","Integer"));
            a.add(Pair.of("documentary","Integer"));
            a.add(Pair.of("drama","Integer"));
            a.add(Pair.of("fantasy","Integer"));
            a.add(Pair.of("film_noir","Integer"));
            a.add(Pair.of("horror","Integer"));
            a.add(Pair.of("musical","Integer"));
            a.add(Pair.of("mystery","Integer"));
            a.add(Pair.of("romance","Integer"));
            a.add(Pair.of("sci_fi","Integer"));
            a.add(Pair.of("thriller","Integer"));
            a.add(Pair.of("war","Integer"));
            a.add(Pair.of("western","Integer"));
        }
        else if(tableName.equalsIgnoreCase("zipcodes")){
            a.add(Pair.of("zipcode","Integer"));
            a.add(Pair.of("zipcodetype","String"));
            a.add(Pair.of("city","String"));
            a.add(Pair.of("state","String"));
        }
        else if(tableName.equalsIgnoreCase("users")){
            a.add(Pair.of("userid","Integer"));
            a.add(Pair.of("age","Integer"));
            a.add(Pair.of("gender","String"));
            a.add(Pair.of("occupation","String"));
            a.add(Pair.of("zipcode","Integer"));
        }
        return a;
    }
    public static int getIdx(String filename,String columnname){
        int i = 0;
        for(Pair<String,String> x : Table.getKeys(filename)){
            if(x.getKey().equalsIgnoreCase(columnname)){
                return i;
            }
            i++;
        }
        return -1;
    }
    Object getColumnValue(String columnName) {
        for (Pair<Pair<String,String>,String> x: row) {
            if(x.getKey().getKey().equalsIgnoreCase(columnName))
                return x.getValue();
        }
        return null;
    }

    String groupByString(String[] columns) {
        return null;
    }

    Boolean checkColumnValue(String columnName, String value) {
        for (Pair<Pair<String,String>,String> x: row) {
            //TODO Add all Data Type Check Values
            if(x.getKey().getKey().equalsIgnoreCase(columnName) && x.getValue().equalsIgnoreCase(value))
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
