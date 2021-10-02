package com.cloudybytes.hadoopmapredimpl;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.tables.Table;
public class InputReducerJoin extends Reducer<Text, Text, Text, Text>
    {
        private static String commonSeparator;
        private static String typeofjoin;
        public void setup(Context context)
        {
            Configuration configuration = context.getConfiguration();
//Retrieving the common file separator from context for output file.
            commonSeparator=configuration.get("Separator.Common");
            typeofjoin = configuration.get("Jointype");
            System.out.println(typeofjoin + " " + context.getConfiguration().get("Name.File2") + " " + context.getConfiguration().get("Name.File1"));
        }

        @Override
        public void reduce(Text key, Iterable<Text>
                textValues, Context context) throws IOException, InterruptedException
        {
            StringBuilder stringBuilder = new StringBuilder();
            String[] firstFileValues=null, secondFileValues=null;
            String[] stringValues;
            for (Text textValue : textValues)
            {
                stringValues = textValue.toString().split(commonSeparator);
                if("F1".equalsIgnoreCase(stringValues[0]))
                {
                    firstFileValues=Arrays.copyOf(stringValues, stringValues.length);
                }
                if("F2".equalsIgnoreCase(stringValues[0]))
                {
                    secondFileValues = Arrays.copyOf(stringValues, stringValues.length);
                }
            }
            
            if(firstFileValues != null && secondFileValues != null) {
                for (int index = 1; index < firstFileValues.length; index++) {
                    stringBuilder.append(firstFileValues[index] + commonSeparator);
                }
                for (int index = 1; index < secondFileValues.length; index++) {
                    stringBuilder.append(secondFileValues[index] + commonSeparator);
                }
                context.write(key, new Text(stringBuilder.toString()));
            }
            else if(firstFileValues != null && typeofjoin.equals("leftouter")){
                for (int index = 1; index < firstFileValues.length; index++) {
                    stringBuilder.append(firstFileValues[index] + commonSeparator);
                }
                Integer x = Table.getKeys(context.getConfiguration().get("Name.File2")).size();
                for (int index = 1; index < x;index++){
                    stringBuilder.append("NULL" + commonSeparator);
                }
                context.write(key, new Text(stringBuilder.toString()));
            }
        }
    }

