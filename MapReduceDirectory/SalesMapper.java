package SalesCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	public static boolean isNumeric(String strNum) {
		if (strNum == null) {
			return false;
		}
		try {
			double d = Double.parseDouble(strNum);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

	public void map(LongWritable key, Text value, OutputCollector <Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] parts = line.split(",");
		if(isNumeric(parts[8] )){
            String month = parts[5];
            double price = Double.parseDouble(parts[8]);
            int weekNights = Integer.parseInt(parts[2]);
            int weekendNights = Integer.parseInt(parts[1]);
            double totalRevenue = price * (weekNights + weekendNights);

            output.collect(new Text(month),new DoubleWritable(totalRevenue));
		}
        
    }

}
