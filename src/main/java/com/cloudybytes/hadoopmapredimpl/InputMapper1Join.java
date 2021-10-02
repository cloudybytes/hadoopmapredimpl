package com.cloudybytes.hadoopmapredimpl;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.tables.Table;
public class InputMapper1Join extends Mapper<LongWritable, Text, Text, Text> {

        private static String separator;
        private static String commonSeparator;
        private static String FILE_TAG="F1";
        private static String filename;
        private static Integer joinColumn;
        public void setup(Context context)
        {
            Configuration configuration = context.getConfiguration();
//Retrieving the file separator from context for file1.
            separator = configuration.get("Separator.File1");
            filename = configuration.get("Name.File1");
//Retrieving the file separator from context for writing the data to reducer.
            commonSeparator=configuration.get("Separator.Common");
            joinColumn = Integer.parseInt(configuration.get("JColumn1"));
        }
        @Override
        public void map(LongWritable rowKey, Text value,
            Context context) throws IOException, InterruptedException
        {
            String[] values = value.toString().split(separator);
            StringBuilder stringBuilder = new StringBuilder();
            for(int index=0;index<values.length;index++)
            {
                if(index != joinColumn)
                    stringBuilder.append(values[index]+commonSeparator);
            }
            if(values[joinColumn] != null && !"NULL".equalsIgnoreCase(values[joinColumn]))
            {
                context.write(new Text(values[joinColumn]), new Text(FILE_TAG+commonSeparator+stringBuilder.toString()));
            }
        }
    }

