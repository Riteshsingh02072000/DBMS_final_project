package SalesCountry;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class SalesCountryDriver {
	public static void main(String[] args) {
		JobClient my_client = new JobClient();
		// Create a configuration object for the job
		JobConf job_conf = new JobConf(SalesCountryDriver.class);

		job_conf.setJobName("SalePerCountry");

		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(DoubleWritable.class);

		job_conf.setMapperClass(SalesCountry.SalesMapper.class);
		job_conf.setReducerClass(SalesCountry.SalesCountryReducer.class);

		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		
		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

		my_client.setConf(job_conf);
		try {
			JobClient.runJob(job_conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
