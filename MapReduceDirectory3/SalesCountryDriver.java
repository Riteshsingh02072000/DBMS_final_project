package SalesCountry;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.io.IOException;
import java.util.*;


public class SalesCountryDriver {


	 private static List<String[]> readDataFromFile(String inputFile) throws IOException {
        List<String[]> data = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split("\t");
                data.add(columns);
            }
        }

        return data;
    }

    private static void writeDataToFile(List<String[]> data, String outputFile) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            for (String[] columns : data) {
                writer.write(String.join("\t", columns));
                writer.newLine();
            }
        }
    }
	

	public static void main(String[] args) {
		JobClient my_client = new JobClient();
		JobConf job_conf = new JobConf(SalesCountryDriver.class);

		job_conf.setJobName("SalePerCountry");

		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(DoubleWritable.class);

		job_conf.setMapperClass(SalesCountry.SalesMapper.class);
		job_conf.setReducerClass(SalesCountry.SalesCountryReducer.class);

		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		
		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileInputFormat.setInputPaths(job_conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[2]));
		

		my_client.setConf(job_conf);
		try {
			// Run the job 
			JobClient.runJob(job_conf);
		}
		catch(Exception e)
		{

		}
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9820");

	}
}