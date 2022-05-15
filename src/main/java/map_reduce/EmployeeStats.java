package map_reduce;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class EmployeeStats {
public static void main(String[] args) throws Exception {
args = new String[] { "employee_info.csv",
"employee_stats" };
/* delete the output directory before running the job */
FileUtils.deleteDirectory(new File(args[1]));
/* set the hadoop system parameter */
//System.setProperty("hadoop.home.dir", "Replace this string with hadoop home directory location");
if (args.length != 2) {
System.err.println("Please specify the input and output path");
System.exit(-1);
}
Configuration conf = new Configuration();
Job job = Job.getInstance(conf);
job.setJarByClass(EmployeeStats.class);
job.setJobName("Find_Max_Min_Avg");
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.setMapperClass(EmployeeMinMaxCountMapper.class);
job.setReducerClass(EmployeeMinMaxCountReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(CustomMinMaxTuple.class);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}