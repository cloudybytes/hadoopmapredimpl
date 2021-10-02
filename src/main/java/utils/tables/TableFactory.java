package utils.tables;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

public class TableFactory {

    public static Table getTable(String tableName, String row, ArrayList<Pair<String,String>> keys) {
        String[] values = row.split(",");
        return new Table(values,keys);
    }
}