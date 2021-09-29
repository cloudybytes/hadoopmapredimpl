package utils.tables;

import java.util.ArrayList;

public class UserTable implements Table{
    Integer userid,age;
    String gender,occupation,zipcode;

    public UserTable(String userid, String age, String gender, String occupation, String zipcode) {
        this.userid = Integer.valueOf(userid);
        this.age = Integer.valueOf(age);
        this.gender = gender;
        this.occupation = occupation;
        this.zipcode = zipcode;
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
