package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Report2 {

    public static final Class OUTPUT_KEY_CLASS = LongWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, LongWritable, IntWritable> { 
	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();

        @Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split(" "); // Split our value based on spaces.
            LongWritable resp_code = new LongWritable();
            resp_code.set(Long.parseLong(sa[8]));
            context.write(resp_code, one);
        }
    }

    public static class ReducerImpl extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(LongWritable resp_code, Iterable<IntWritable> intOne, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()) {
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(resp_code, result);
       }
    }

}
