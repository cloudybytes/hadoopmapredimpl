package utils.tables;

import java.util.ArrayList;

public class TableFactory {

    public static Table getTable(String tableName, String row, ArrayList<String> keys) {
        String[] values = row.split(",");
        return new Table(values,keys);
    }
}