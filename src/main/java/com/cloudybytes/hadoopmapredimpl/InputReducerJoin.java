package com.cloudybytes.hadoopmapredimpl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InputReducerJoin extends Reducer<Text, Text, Text, Text>
    {
        private static String commonSeparator;
        private static String typeofjoin;
        public void setup(Context context)
        {
            Configuration configuration = context.getConfiguration();
            commonSeparator=configuration.get("Separator.Common");
            typeofjoin = configuration.get("Jointype");
            System.out.println(typeofjoin + " " + context.getConfiguration().get("Name.File2") + " " + context.getConfiguration().get("Name.File1"));
        }

        @Override
        public void reduce(Text key, Iterable<Text>
                textValues, Context context) throws IOException, InterruptedException
        {
            StringBuilder stringBuilder = new StringBuilder();
            ArrayList<String[]> firstFileValues= new ArrayList<>();
            ArrayList<String[]> secondFileValues = new ArrayList<>();
            String[] stringValues;
            for (Text textValue : textValues)
            {
                stringValues = textValue.toString().split(commonSeparator);
                if("F1".equalsIgnoreCase(stringValues[0]))
                {
                    firstFileValues.add(Arrays.copyOf(stringValues, stringValues.length));
                }
                if("F2".equalsIgnoreCase(stringValues[0]))
                {
                    secondFileValues.add(Arrays.copyOf(stringValues, stringValues.length));
                }
            }
            int x = Integer.parseInt(context.getConfiguration().get("Size.File2"));
            if(firstFileValues.size() != 0 && secondFileValues.size() != 0) {
                for(String[] file : firstFileValues) {
                    for (int index = 1; index < file.length; index++) {
                        stringBuilder.append(commonSeparator).append(file[index]);
                    }
                    for (String[] file2 : secondFileValues)
                    {
                        for (int index = 1; index < file2.length; index++) {
                            stringBuilder.append(commonSeparator).append(file2[index]);
                        }
                        context.write(key, new Text(stringBuilder.toString()));
                        stringBuilder.delete(0, stringBuilder.length());
                    }
                }
            }
            else if(firstFileValues.size() != 0 && typeofjoin.equals("leftouter")){
                for(String[] file : firstFileValues) {
                    for (int index = 1; index < file.length; index++) {
                        stringBuilder.append(commonSeparator).append(file[index]);
                    }
                    for (int index = 1; index < x; index++) {
                        stringBuilder.append(commonSeparator).append("NULL");
                    }
                    context.write(key, new Text(stringBuilder.toString()));
                    stringBuilder.delete(0,stringBuilder.length());
                }
            }
        }
    }