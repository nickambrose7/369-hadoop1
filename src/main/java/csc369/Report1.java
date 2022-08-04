package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Report1 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();

        @Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split(" "); // Split our value based on spaces.
            Text url = new Text();
            url.set(sa[6]); // after the split url should be at spot 6
            context.write(url, one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text word, Iterable<IntWritable> intOne, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()) {
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(word, result); // (URL, how man times we see our url)
       }
    }

}
