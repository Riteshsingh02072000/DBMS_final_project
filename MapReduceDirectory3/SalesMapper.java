package SalesCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	public void map(LongWritable key, Text value, OutputCollector <Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] parts = line.split("\t");
		
            String month = parts[0];
            
            double totalRevenue = Double.parseDouble(parts[1]);

            output.collect(new Text(month),new DoubleWritable(totalRevenue));
		
        
    }

}
