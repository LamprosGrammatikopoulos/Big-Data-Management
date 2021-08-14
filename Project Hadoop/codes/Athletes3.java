import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Athletes
{
    public static int max1=0, max2=0, max3=0;
    //---------------------------------------------------1------------------------------------------------------------------------
    public static class FemalesMapper extends Mapper<LongWritable,Text,Text,IntWritable>
    {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
      public void map(LongWritable key, Text inputCSVLine, Context context) throws IOException, InterruptedException
      {
        int counter = 0;
	String all = null;
	if(((LongWritable)key).get()==0)
	{
	    //Skip the header line (it starts at offset 0)
	    return;
	}
        String[] fields = inputCSVLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        for(String tmp : fields)
        {
            if(counter == 2)
            {
        	word.set(tmp);
        	if(tmp.compareTo("F") == 0)
        	{
        	    all = fields[8] + "," + fields[6] + "," + fields[7] + "," + fields[12]; 
        	    word.set(all);
        	    context.write(word,one);
        	}
            }
            counter++;
        }
      }
    }
    public static class FemalesReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
	private IntWritable result = new IntWritable();
	public void reduce(Text word, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
	    int sum = 0;
	    for (IntWritable val : values)
	    {
		sum += val.get();
	    }
	    result.set(sum);
	    context.write(word,result);
	}
    }
    
    //---------------------------------------------------2------------------------------------------------------------------------
    public static class TabsMapper extends Mapper<LongWritable,Text,Text,NullWritable>
    {
      public void map(LongWritable key, Text inputLine, Context context) throws IOException, InterruptedException
      {
	  String femaleAll = null;
	  String[] input = inputLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	  String[] tmp = input[3].toString().split("\t");
	  femaleAll = input[0] + "," + input[1] + "," + input[2] + "," + tmp[1] + "," + tmp[0];
	  context.write(new Text(femaleAll),NullWritable.get());
      }
    }
    public static class TabsReducer extends Reducer<Text,NullWritable,Text,NullWritable>
    {
	public void reduce(Text femaleAll, NullWritable n,Context context) throws IOException, InterruptedException
	{
	    context.write(femaleAll,NullWritable.get());
	}
    }
    
    //---------------------------------------------------3------------------------------------------------------------------------
    public static class DuplicatesMapper extends Mapper<LongWritable,Text,Text,Text>
    {
      public void map(LongWritable key, Text inputLine, Context context) throws IOException, InterruptedException
      {
	  String gamesteamnoc = null,femaleAll = null;
	  String[] input = inputLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	  gamesteamnoc = input[0] + "," + input[1] + "," + input[2];
	  femaleAll = input[0] + "," + input[1] + "," + input[2] + "," + input[3] + "," + input[4];
	  context.write(new Text(gamesteamnoc),new Text(femaleAll));
      }
    }
    public static class DuplicatesReducer extends Reducer<Text,Text,Text,Text>
    {
	private Text result = new Text();
	public void reduce(Text gamesteamnoc, Iterable<Text> femaleAll, Context context) throws IOException, InterruptedException
	{
	    for (Text val : femaleAll)
	    {
		String tmp[]=val.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		result.set(tmp[0]+","+tmp[1]+","+tmp[2]+","+tmp[3]+","+tmp[4]);
	    }
	    context.write(gamesteamnoc,result);
	}
    }
    
    //---------------------------------------------------4------------------------------------------------------------------------
    public static class RemoveGTNMapper extends Mapper<LongWritable,Text,Text,NullWritable>
    {
      public void map(LongWritable key, Text inputLine, Context context) throws IOException, InterruptedException
      {
	  String femaleAll = null;
	  String[] input = inputLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	  String[] tmp = input[2].toString().split("\t");
	  femaleAll = tmp[1] + "," + input[3] + "," + input[4] + "," + input[5] + "," + input[6];
	  context.write(new Text(femaleAll),NullWritable.get());
      }
    }
    public static class RemoveGTNReducer extends Reducer<Text,NullWritable,Text,NullWritable>
    {
	public void reduce(Text femaleAll, NullWritable n, Context context) throws IOException, InterruptedException
	{
	    context.write(femaleAll,NullWritable.get());
	}
    }
    

    //---------------------------------------------------5------------------------------------------------------------------------
    public static class ThreeGamesMapper extends Mapper<LongWritable,Text,Text,NullWritable>
    {
	public void map(LongWritable key, Text inputLine, Context context) throws IOException, InterruptedException
	{
	    ArrayList<String> femaleAll = new ArrayList<String>();
	    String[] input = inputLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	    String tmpline = input[0] + "," + input[1] + "," + input[2] + "," + input[3] + "," + input[4];
	    femaleAll.add(tmpline);
	    for(String line : femaleAll)
	    {
		String[]fields = line.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		int Participations = Integer.parseInt(fields[3]);
		if(Participations>max1)
		{
		    max1 = Participations;
		}
	    }
	    for(String line : femaleAll)
	    {
		String[] fields = line.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		int Participations = Integer.parseInt(fields[3]);
		if(Participations>max2 && Participations!=max1)
		{ 
		    max2 = Participations;
		}
	    }
	    for(String line : femaleAll)
	    {
		String[] fields = line.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		int Participations = Integer.parseInt(fields[3]);
		if(Participations>max3 && Participations!=max1 && Participations!=max2)
		{
		    max3 = Participations;
		}
	    }
	    for(String line : femaleAll)
	    {
		String[] fields = line.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		if((Integer.parseInt(fields[3])==max1 || Integer.parseInt(fields[3])==max2 || Integer.parseInt(fields[3])==max3))
		{
		    String female_All = fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3] + "," + fields[4];
		    context.write(new Text(female_All),NullWritable.get());
		}
	    }
	}
    }
    public static class ThreeGamesReducer extends Reducer<Text,NullWritable,Text,NullWritable>
    {
	public void reduce(Text femaleAll, NullWritable m, Context context) throws IOException, InterruptedException
	{
	    context.write(femaleAll,NullWritable.get());
	}
    }
    
    //--------------------------------------------------------Comprator-------------------------------------------------------------------
    public static class KeyComprator extends WritableComparator
    {
	 protected KeyComprator()
	 {
	     super(Text.class, true);
	 }
	 @SuppressWarnings("rawtypes")
	 @Override
	 public int compare(WritableComparable w1, WritableComparable w2)
	 {
        	 Text t1 = (Text) w1;
        	 Text t2 = (Text) w2;
        	 String[] fields1 = t1.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        	 String[] fields2 = t2.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        	 String t1Base = fields1[0];
        	 String t2Base = fields2[0];
        	 int comp = t1Base.compareTo(t2Base);
        	 if(comp != 0)
        	 {
        	     return comp;
        	 }
                 else
                 {
                     t1Base = fields1[3];
            	     t2Base = fields2[3];
            	     int comp2 = t1Base.compareTo(t2Base);
            	     if(comp2 != 0)
            	     {
            		return -1*comp2;
            	     }
            	     else
            	     {
            		t1Base = fields1[1];
               	     	t2Base = fields2[1];
               	     	int comp3 = t1Base.compareTo(t2Base);
               	     	return comp3;
            	     }
                 }
	 }
    }
    
    //---------------------------------------------------------------------M A I N---------------------------------------------------------------------
    public static void main(String[] args) throws Exception
    {
      if(args.length != 7)
      {
	  System.err.println("Please provide input and output paths.");
	  System.exit(-1);
      }
      
      Configuration conf1 = new Configuration();
      Job job1 = Job.getInstance(conf1, "CountingFemales");
      job1.setJarByClass(Athletes.class);
      job1.setMapperClass(FemalesMapper.class);
      job1.setCombinerClass(FemalesReducer.class);
      job1.setReducerClass(FemalesReducer.class);
      job1.setMapOutputKeyClass(Text.class);
      job1.setMapOutputValueClass(IntWritable.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job1, new Path(args[1]));
      FileOutputFormat.setOutputPath(job1, new Path(args[2]));
            
      job1.waitForCompletion(true);
      
      Configuration conf2 = new Configuration();
      Job job2 = Job.getInstance(conf2, "RevomingTabs-ReversingLastTwo-Sorting");
      job2.setJarByClass(Athletes.class);
      job2.setMapperClass(TabsMapper.class);
      job2.setCombinerClass(TabsReducer.class);
      job2.setReducerClass(TabsReducer.class);
      job2.setSortComparatorClass(KeyComprator.class);
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(NullWritable.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(NullWritable.class);
      FileInputFormat.addInputPath(job2, new Path(args[2]+"/part-r-00000"));
      FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            
      job2.waitForCompletion(true);
      
      Configuration conf3 = new Configuration();
      Job job3 = Job.getInstance(conf3, "RevomingTeamsWithSameTeamName");
      job3.setJarByClass(Athletes.class);
      job3.setMapperClass(DuplicatesMapper.class);
      job3.setCombinerClass(DuplicatesReducer.class);
      job3.setReducerClass(DuplicatesReducer.class);
      job3.setMapOutputKeyClass(Text.class);
      job3.setMapOutputValueClass(Text.class);
      job3.setOutputKeyClass(Text.class);
      job3.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job3, new Path(args[3]+"/part-r-00000"));
      FileOutputFormat.setOutputPath(job3, new Path(args[4]));
      
      job3.waitForCompletion(true);
      
      Configuration conf4 = new Configuration();
      Job job4 = Job.getInstance(conf4, "RevomingTeamGamesNocFromData-Sorting");
      job4.setJarByClass(Athletes.class);
      job4.setMapperClass(RemoveGTNMapper.class);
      job4.setCombinerClass(RemoveGTNReducer.class);
      job4.setReducerClass(RemoveGTNReducer.class);
      job4.setSortComparatorClass(KeyComprator.class);
      job4.setMapOutputKeyClass(Text.class);
      job4.setMapOutputValueClass(NullWritable.class);
      job4.setOutputKeyClass(Text.class);
      job4.setOutputValueClass(NullWritable.class);
      FileInputFormat.addInputPath(job4, new Path(args[4]+"/part-r-00000"));
      FileOutputFormat.setOutputPath(job4, new Path(args[5]));
      
      job4.waitForCompletion(true);
      
      Configuration conf5 = new Configuration();
      Job job5 = Job.getInstance(conf5, "RevomingTeamsWithLessFemaleParticipations");
      job5.setJarByClass(Athletes.class);
      job5.setMapperClass(ThreeGamesMapper.class);
      job5.setCombinerClass(ThreeGamesReducer.class);
      job5.setReducerClass(ThreeGamesReducer.class);
      job5.setSortComparatorClass(KeyComprator.class);
      job5.setMapOutputKeyClass(Text.class);
      job5.setMapOutputValueClass(NullWritable.class);
      job5.setOutputKeyClass(Text.class);
      job5.setOutputValueClass(NullWritable.class);
      FileInputFormat.addInputPath(job5, new Path(args[5]+"/part-r-00000"));
      FileOutputFormat.setOutputPath(job5, new Path(args[6]));
      
      System.exit(job5.waitForCompletion(true) ? 0 : 1);
    }
}



