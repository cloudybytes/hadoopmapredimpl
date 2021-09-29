package utils.tables;

import java.util.ArrayList;
import java.util.Date;

public class MovieTable implements Table{
    Integer movieId;
    String title;
    long releaseDate;
    Boolean unknown,action,adventure,animation,children,comedy,crime,documentary,drama,fantasy,film_noir,horror,musical,mystery,romance,sci_fi,thriller,war,western;

    public MovieTable(String movieId, String title, String releaseDate, String unknown, String action, String adventure, String animation, String children, String comedy, String crime, String documentary, String drama, String fantasy, String film_noir, String horror, String musical, String mystery, String romance, String sci_fi, String thriller, String war, String western) {
        this.movieId = Integer.valueOf(movieId);
        this.title = title;
        this.releaseDate = Date.parse(releaseDate);
        this.unknown = Boolean.valueOf(unknown);
        this.action = Boolean.valueOf(action);
        this.adventure = Boolean.valueOf(adventure);
        this.animation = Boolean.valueOf(animation);
        this.children = Boolean.valueOf(children);
        this.comedy = Boolean.valueOf(comedy);
        this.crime = Boolean.valueOf(crime);
        this.documentary = Boolean.valueOf(documentary);
        this.drama = Boolean.valueOf(drama);
        this.fantasy = Boolean.valueOf(fantasy);
        this.film_noir = Boolean.valueOf(film_noir);
        this.horror = Boolean.valueOf(horror);
        this.musical = Boolean.valueOf(musical);
        this.mystery = Boolean.valueOf(mystery);
        this.romance = Boolean.valueOf(romance);
        this.sci_fi = Boolean.valueOf(sci_fi);
        this.thriller = Boolean.valueOf(thriller);
        this.war = Boolean.valueOf(war);
        this.western = Boolean.valueOf(western);
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
