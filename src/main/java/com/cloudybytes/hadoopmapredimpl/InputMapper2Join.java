package com.cloudybytes.hadoopmapredimpl;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.tables.Table;

public class InputMapper2Join  extends Mapper <LongWritable, Text, Text, Text>
    {
        private static String separator;
        private static String commonSeparator;
        private static String FILE_TAG="F2";
        private static String filename;

        public void setup(Context context)
        {
            Configuration configuration = context.getConfiguration();
//Retrieving the file separator from context for file2.
            separator = configuration.get("Separator.File2");
            filename = configuration.get("Name.File2");
//Retrieving the file separator from context for writing the data to reducer.
            commonSeparator=configuration.get("Separator.Common");
        }
        @Override
        public void map(LongWritable rowKey, Text value,
                        Context context) throws IOException, InterruptedException
        {
            String[] values = value.toString().split(separator);
            StringBuilder stringBuilder = new StringBuilder();
            int remidx = 0;
            remidx = Table.getIdx(filename,"zipcode");
            for(int index=0;index<values.length;index++)
            {
                if(index != remidx)
                    stringBuilder.append(values[index]+commonSeparator);
            }
            if(values[remidx] != null && !"NULL".equalsIgnoreCase(values[remidx]))
            {
                context.write(new Text(values[remidx]), new Text(FILE_TAG+commonSeparator+stringBuilder.toString()));
            }
        }
    }
