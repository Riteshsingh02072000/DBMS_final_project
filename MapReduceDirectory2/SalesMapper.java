package SalesCountry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
	private static Map<String, Integer> monthMap;

    static {
        monthMap = new HashMap<>();
        monthMap.put("January", 1);
        monthMap.put("February", 2);
        monthMap.put("March", 3);
        monthMap.put("April", 4);
        monthMap.put("May", 5);
        monthMap.put("June", 6);
        monthMap.put("July", 7);
        monthMap.put("August", 8);
        monthMap.put("September", 9);
        monthMap.put("October", 10);
        monthMap.put("November", 11);
        monthMap.put("December", 12);
    }

    public static int getMonthNumber(String monthName) {
        Integer monthNumber = monthMap.get(monthName);
        if (monthNumber == null) {
            throw new IllegalArgumentException("Invalid month name: " + monthName);
        }
        return monthNumber;
    }
	
	public void map(LongWritable key, Text value, OutputCollector <Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] parts = line.split(",");
		if(isNumeric(parts[11] )){
            String month = String.valueOf(getMonthNumber(parts[4]));
            double price = Double.parseDouble(parts[11]);
            int weekNights = Integer.parseInt(parts[8]);
            int weekendNights = Integer.parseInt(parts[7]);
            double totalRevenue = price * (weekNights + weekendNights);

            output.collect(new Text(month),new DoubleWritable(totalRevenue));
		}
        
    }

}
