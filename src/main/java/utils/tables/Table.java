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
            a.add(Pair.of("userid","Long"));
            a.add(Pair.of("movieid","Long"));
            a.add(Pair.of("rating","Long"));
            a.add(Pair.of("timestamp","Long"));
        }
        else if(tableName.equalsIgnoreCase("movies")){
            a.add(Pair.of("movieid","Long"));
            a.add(Pair.of("title","Long"));
            a.add(Pair.of("releasedate","Date"));
            a.add(Pair.of("unknown","Long"));
            a.add(Pair.of("action","Long"));
            a.add(Pair.of("adventure","Long"));
            a.add(Pair.of("animation","Long"));
            a.add(Pair.of("children","Long"));
            a.add(Pair.of("comedy","Long"));
            a.add(Pair.of("crime","Long"));
            a.add(Pair.of("documentary","Long"));
            a.add(Pair.of("drama","Long"));
            a.add(Pair.of("fantasy","Long"));
            a.add(Pair.of("film_noir","Long"));
            a.add(Pair.of("horror","Long"));
            a.add(Pair.of("musical","Long"));
            a.add(Pair.of("mystery","Long"));
            a.add(Pair.of("romance","Long"));
            a.add(Pair.of("sci_fi","Long"));
            a.add(Pair.of("thriller","Long"));
            a.add(Pair.of("war","Long"));
            a.add(Pair.of("western","Long"));
        }
        else if(tableName.equalsIgnoreCase("zipcodes")){
            a.add(Pair.of("zipcode","Long"));
            a.add(Pair.of("zipcodetype","String"));
            a.add(Pair.of("city","String"));
            a.add(Pair.of("state","String"));
        }
        else if(tableName.equalsIgnoreCase("users")){
            a.add(Pair.of("userid","Long"));
            a.add(Pair.of("age","Long"));
            a.add(Pair.of("gender","String"));
            a.add(Pair.of("occupation","String"));
            a.add(Pair.of("zipcode","Long"));
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
    public Pair<String,String> getColumnValue(String columnName) {
        for (Pair<Pair<String,String>,String> x: row) {
            if(x.getKey().getKey().equalsIgnoreCase(columnName))
                return Pair.of(x.getValue(),x.getKey().getValue());
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
