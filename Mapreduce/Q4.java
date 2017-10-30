import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Q4 
{
	public static class Q4Mapper extends Mapper<LongWritable,Text,Text,LongWritable>	
	{
		LongWritable one =new LongWritable(1); 
		public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException,ArrayIndexOutOfBoundsException
		{  	
		  try
		  {
			  if(key.get()> 0)
			  {
			   String[] token=values.toString().split("\t");	  
			   if (!token[1].equals("NA") && token[1] != null && !token[2].equals("NA") && token[2] != null && !token[7].equals("NA") && token[7] != null)
				{	Text answer= new Text(token[2]+'\t'+token[7]);
			  		context.write(answer,one);
				}
			  }
		  }
		  catch (ArrayIndexOutOfBoundsException e) 
		  {}
	    }
	}
	public static class Q4Reducer extends Reducer < Text, LongWritable, NullWritable, Text > 
	{
	    private TreeMap < LongWritable,Text > Top5Employers = new TreeMap < LongWritable,
	    Text > ();
	    long sum = 0;
	    public void reduce(Text key, Iterable < LongWritable > values, Context context) throws IOException,
	    InterruptedException {
	        sum = 0;
	        for (LongWritable val: values) {
	            sum += val.get();
	        }
	        Top5Employers.put(new LongWritable(sum), new Text(key + "," + sum));
	        if (Top5Employers.size() > 5)
	            Top5Employers.remove(Top5Employers.firstKey());

	    }
	    protected void cleanup(Context context) throws IOException,
	    InterruptedException {
	        for (Text t: Top5Employers.descendingMap().values())
	            context.write(NullWritable.get(), t);

	    }
	}
	public static class Q4Partitioner extends Partitioner < Text, LongWritable > 
	{
	    public int getPartition(Text key, LongWritable value, int numReduceTasks) 
	    {
	        String[] str = key.toString().split("\t");
	        if (str[1].equals("2011"))
	        {
	            return 0;
	        }
	        else if (str[1].equals("2012"))
	        {
	            return 1;
	        }
	        else if (str[1].equals("2013"))
	        {
	            return 2;
	        }
	        else if (str[1].equals("2014"))
	        {
	            return 3;
	        }
	        else if (str[1].equals("2015"))
	        {
	            return 4;
	        }
	        else if (str[1].equals("2016"))
	        {
	            return 5;
	        }
	        else
	        {
	            return 6;
	        }
	    }
	}
	public static void main(String args[]) throws Exception
	{
	Configuration conf= new Configuration();
	Job job= Job.getInstance(conf,"Question 4");
	job.setJarByClass(Q4.class);
	FileInputFormat.setInputPaths(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));	
	job.setMapperClass(Q4Mapper.class);
	job.setPartitionerClass(Q4Partitioner.class);
	job.setReducerClass(Q4Reducer.class);
	job.setNumReduceTasks(7);
	job.setInputFormatClass(TextInputFormat.class);	
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);
	System.exit(job.waitForCompletion(true)?1:0);
	}	
}