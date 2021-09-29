package utils.tables;

import java.util.ArrayList;

public class ZipTable implements Table{
    String zipcode, zipCodeType,city,state;
    ZipTable(String zipcode,String zipCodeType,String city,String state){
        this.zipcode = zipcode;
        this.zipCodeType = zipCodeType;
        this.city = city;
        this.state = state;
    }
    @Override
    public Object getColumnValue(String columnName) {
        return null;
    }

    @Override
    public String groupByString(String[] columns) {
        return null;
    }

    @Override
    public Boolean checkColumnValue(String columnName, String value) {
        return null;
    }

    @Override
    public Object getAggregate(String operation, String column, ArrayList<Table> arr) {
        return null;
    }

    @Override
    public Boolean compareAggregate(String column, String operation, String comparisonOperator, String value, ArrayList<Table> arr) {
        return null;
    }
}
