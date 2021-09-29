package utils.tables;

import java.util.ArrayList;

public class RatingTable implements Table{
    Integer userId,movieId,rating;
    String timeStamp;
    RatingTable(String userId,String movieId,String rating,String timeStamp){
        this.userId = Integer.valueOf(userId);
        this.movieId = Integer.valueOf(movieId);
        this.rating = Integer.valueOf(rating);
        this.timeStamp = timeStamp;
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
