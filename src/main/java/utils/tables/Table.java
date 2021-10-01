package utils.tables;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;


public class Table {
    ArrayList<Pair<String,String>> row;
    Table(String[] vals,ArrayList<String> keys){
        row = new ArrayList<>();
        int i = 0;
        for (String key:keys) {
            row.add(Pair.of(key,vals[i]));
            i++;
        }
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
