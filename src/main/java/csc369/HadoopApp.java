package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> <input dir> <output dir>");
	    System.exit(-1);
	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
        } else if ("AccessLog2".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog2.ReducerImpl.class);
	    job.setMapperClass(AccessLog2.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog2.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog2.OUTPUT_VALUE_CLASS);
		} else if ("Report1".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report1.ReducerImpl.class);
			job.setMapperClass(Report1.MapperImpl.class);
			job.setOutputKeyClass(Report1.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report1.OUTPUT_VALUE_CLASS);
		} 
		else if ("Report1b".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report1b.ReducerImpl.class);
			job.setMapperClass(Report1b.MapperImpl.class);
			job.setOutputKeyClass(Report1b.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report1b.OUTPUT_VALUE_CLASS);
		} 
		else if ("Report2".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report2.ReducerImpl.class);
			job.setMapperClass(Report2.MapperImpl.class);
			job.setOutputKeyClass(Report2.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report2.OUTPUT_VALUE_CLASS);
		} 
		else if ("Report2b".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report2b.ReducerImpl.class);
			job.setMapperClass(Report2b.MapperImpl.class);
			job.setOutputKeyClass(Report2b.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report2b.OUTPUT_VALUE_CLASS);
		} 
		else if ("Report3".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report3.ReducerImpl.class);
			job.setMapperClass(Report3.MapperImpl.class);
			job.setOutputKeyClass(Report3.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report3.OUTPUT_VALUE_CLASS);
		}
		else if ("Report4".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report4.ReducerImpl.class);
			job.setMapperClass(Report4.MapperImpl.class);
			job.setOutputKeyClass(Report4.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report4.OUTPUT_VALUE_CLASS);
		}
		else if ("Report4b".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report4b.ReducerImpl.class);
			job.setMapperClass(Report4b.MapperImpl.class);
			job.setOutputKeyClass(Report4b.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report4b.OUTPUT_VALUE_CLASS);
		}
		else if ("Report5".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report5.ReducerImpl.class);
			job.setMapperClass(Report5.MapperImpl.class);
			job.setOutputKeyClass(Report5.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report5.OUTPUT_VALUE_CLASS);
		}
		else if ("Report5b".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report5b.ReducerImpl.class);
			job.setMapperClass(Report5b.MapperImpl.class);
			job.setOutputKeyClass(Report5b.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report5b.OUTPUT_VALUE_CLASS);
		}
		else if ("Report6".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report6.ReducerImpl.class);
			job.setMapperClass(Report6.MapperImpl.class);
			job.setOutputKeyClass(Report6.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report6.OUTPUT_VALUE_CLASS);
		}
		else if ("Report6b".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(Report6b.ReducerImpl.class);
			job.setMapperClass(Report6b.MapperImpl.class);
			job.setOutputKeyClass(Report6b.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(Report6b.OUTPUT_VALUE_CLASS);
		}
		else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
