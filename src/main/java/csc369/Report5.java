package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Report5 {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
        /* this function takes as input each line of our access.log file and emits (hostname, 1) for each line.
        Need to understand that the map function is called separatly on each line in our file. */
	    String[] sa = value.toString().split(" "); // Split our value based on spaces.
	    IntWritable year = new IntWritable();
	    IntWritable month = new IntWritable();
	    year.set(Integer.parseInt(sa[3].substring(8, 12))); // first item in
        String m = sa[3].substring(4, 7);
        if (m.equals("Jan")) {
            month.set(1);
        }
        else if (m.equals("Feb")) {
            month.set(2);
        }
        else if (m.equals("Mar")) {
            month.set(3);
        }
        else if (m.equals("Apr")) {
            month.set(4);
        }
        else if (m.equals("May")) {
            month.set(5);
        }
        else if (m.equals("Jun")) {
            month.set(6);
        }
        else if (m.equals("Jul")) {
            month.set(7);
        }
        else if (m.equals("Aug")) {
            month.set(8);
        }
        else if (m.equals("Sep")) {
            month.set(9);
        }
        else if (m.equals("Oct")) {
            month.set(10);
        }
        else if (m.equals("Nov")) {
            month.set(11);
        }
        else if (m.equals("Dec")) {
            month.set(12);
        }
	    context.write(month, one);
	    context.write(year, one);
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	private IntWritable result = new IntWritable(); // would need to change the type if I wanted 
    //my result to be a different type.
    
        @Override
	protected void reduce(IntWritable hostname, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
        /* This funcition takes a given hostname and a list of 1's for each time the hostname occured. */
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get(); // add up all the ones.
            }
            result.set(sum);
            context.write(hostname, result); // emit the hostname and amount of times it occurs.
            // output file will be tab delimited.
       }
    }
}
