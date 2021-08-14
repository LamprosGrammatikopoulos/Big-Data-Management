import java.io.IOException;
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
    public static String previousgolds = "-1";
    public static String previousmedals = "-1";
    public static class AMapper extends Mapper<LongWritable,Text,Text,Text>
    {
      private Text medals = new Text();
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
            if(counter == 14)
            {
        	word.set(tmp);
        	all = fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3] + "," + fields[6] + "," + fields[12] + "," + fields[8] + ","; 
        	if(tmp.compareTo("Gold") == 0)
        	{
        	    medals.set("1,0,0");
        	    word.set(all);
        	    context.write(word,medals);
        	}
        	else if(tmp.compareTo("Silver") == 0)
        	{
        	    medals.set("0,1,0");
        	    word.set(all);
        	    context.write(word,medals);
        	}
        	else if(tmp.compareTo("Bronze") == 0)
        	{
        	    medals.set("0,0,1");
        	    word.set(all);
        	    context.write(word,medals);
        	}
        	else
        	{
        	    medals.set("0,0,0");
        	    word.set(all);
        	    context.write(word,medals);
        	}
            }
            counter++;
        }
      }
    }
    public static class AReducer extends Reducer<Text,Text,Text,Text>
    {
      private Text result = new Text();
      public void reduce(Text word, Iterable<Text> medals,Context context) throws IOException, InterruptedException
      {
	  String array[]= {"0","0","0"};
	  for (Text val : medals)
	  {
	      String tmp[]=val.toString().split(",");
	      int tmp2[]=new int[3];
	      tmp2[0]=Integer.parseInt(tmp[0])+Integer.parseInt(array[0]);
	      tmp2[1]=Integer.parseInt(tmp[1])+Integer.parseInt(array[1]);
	      tmp2[2]=Integer.parseInt(tmp[2])+Integer.parseInt(array[2]);
	      array[0]=""+tmp2[0];
	      array[1]=""+tmp2[1];
	      array[2]=""+tmp2[2];
	      result.set(array[0]+","+array[1]+","+array[2]);
	  }
	  context.write(word,result);
      }
    }
    
    
    //-----------------------------------------------------------------------2---------------------------------------------------------------------------
    public static class MMapper extends Mapper<LongWritable,Text,Text,Text>
    {
      public void map(LongWritable key, Text inputLine, Context context) throws IOException, InterruptedException
      {
        String[] fields = inputLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        String athleteAll = fields[1] + "," + fields[2] + "," + fields[3]+ "," + fields[4] + "," + fields[5] + "," + fields[6] + "," + fields[7]+ "," + fields[8] + "," + fields[9];
        String sorttext = fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3]+ "," + fields[4] + "," + fields[5];
        context.write(new Text(sorttext),new Text(athleteAll));
      }
    }
    public static class MReducer extends Reducer<Text,Text,Text,Text>
    {
      private Text result = new Text();
      public void reduce(Text sorttext, Iterable<Text> all, Context context) throws IOException, InterruptedException
      {
	  String array[]= {"0","0","0","0"};
	  for (Text val : all)
	  {
	      String tmp[]=val.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	      if(tmp[5].contains(","))
	      {
		  tmp[5]=tmp[5].replace(",","");
	      }
	      if(tmp[6].contains("\t"))
	      {
		  tmp[6]=tmp[6].replace("\t","");
	      }
	      int tmp2[]=new int[3];
	      tmp2[0]=Integer.parseInt(tmp[6])+Integer.parseInt(array[0]);
	      tmp2[1]=Integer.parseInt(tmp[7])+Integer.parseInt(array[1]);
	      tmp2[2]=Integer.parseInt(tmp[8])+Integer.parseInt(array[2]);
	      
	      array[0]=""+tmp2[0];
	      array[1]=""+tmp2[1];
	      array[2]=""+tmp2[2];
	      
	      int sum = Integer.parseInt(array[0])+Integer.parseInt(array[1])+Integer.parseInt(array[2]);
	      result.set(tmp[0]+","+tmp[1]+","+tmp[2]+","+tmp[3]+","+tmp[4]+","+tmp[5]+","+array[0]+","+array[1]+","+array[2]+","+sum);   
	  }
	  context.write(sorttext,result);
      }
    }
    
    
    //-----------------------------------------------------------------------3---------------------------------------------------------------------------
    public static class SortMapper extends Mapper<LongWritable,Text,Text,NullWritable>
    {
      public void map(LongWritable key, Text inputLine, Context context) throws IOException, InterruptedException
      {
	      String[] fields = inputLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	      String[] tmp = fields[5].toString().split("\t");
	      String athlete_All = tmp[1] + "," + fields[6] + "," + fields[7] + "," + fields[8] + "," + fields[9] + "," + fields[10] + "," + fields[11] + "," + fields[12] + "," + fields[13] + "," + fields[14];
	      context.write(new Text(athlete_All),NullWritable.get());
      }
    }
    public static class SortReducer extends Reducer<Text,IntWritable,Text,NullWritable>
    {
	 public void reduce(Iterable<Text> athlete_All, NullWritable n, Context context) throws IOException, InterruptedException
         {
        	 Text result = new Text();
        	 for(Text val : athlete_All)
        	 {
        	     String line = val.toString();
        	     String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        	     result.set(fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3] + "," + fields[4] + "," + fields[5] + "," + fields[6]+ "," + fields[7] + "," + fields[8] + "," + fields[9]);
        	 }
        	 context.write(result,NullWritable.get());
    	 }
    }
    
    //--------------------------------------------------------Comparators------------------------------------------------------------------
    public static class KeyComprator3 extends WritableComparator
    {
	 protected KeyComprator3()
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
        	 String t1Base = fields1[6];
        	 String t2Base = fields2[6];
        	 int comp = t1Base.compareTo(t2Base);
        	 if(comp != 0)
        	 {
        	     return -1*comp;
        	 }
                 else
                 {
                     t1Base = fields1[9];
            	     t2Base = fields2[9];
            	     int comp2 = t1Base.compareTo(t2Base);
            	     if(comp2 != 0)
            	     {
            		return -1*comp2;
            	     }
            	     else
            	     {
            		t1Base = fields1[0];
               	     	t2Base = fields2[0];
               	     	int comp3 = t1Base.compareTo(t2Base);
               	     	return comp3;
            	     }
                 }
	 }
    }
    public static class KeyComprator4 extends WritableComparator
    {
	 protected KeyComprator4()
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
        	 String t1Base = fields1[7];
        	 String t2Base = fields2[7];
        	 int comp = t1Base.compareTo(t2Base);
        	 if(comp != 0)
        	 {
        	     return -1*comp;
        	 }
                 else
                 {
                     t1Base = fields1[10];
            	     t2Base = fields2[10];
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
    
    //-----------------------------------------------------------------------4---------------------------------------------------------------------------
    public static class Top10Mapper extends Mapper<LongWritable,Text,Text,NullWritable>
    {
      int tmpcounter = 1;
      int counter = 0;
      public void map(LongWritable key, Text inputLine, Context context) throws IOException, InterruptedException
      {
	      String[] fields = inputLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	      String athlete_All = fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3] + "," + fields[4] + "," + fields[5] + "," + fields[6] + "," + fields[7] + "," + fields[8] + "," + fields[9];
	      if(counter < 10)
	      {
		  if(counter == 0)
		  {
		      previousgolds = fields[6];
		      previousmedals = fields[9];
		  }
		  if(previousgolds.compareTo(fields[6])!=0 || previousmedals.compareTo(fields[9])!=0)
		  {
			tmpcounter++;
		  }
		  context.write(new Text("" + tmpcounter + "," + athlete_All),NullWritable.get());
		  previousgolds = fields[6];
		  previousmedals = fields[9];
	      }
	      counter++;
      }
    }
    
    public static class Top10Reducer extends Reducer<Text,IntWritable,Text,NullWritable>
    {
	 int counter = 0;
	 public void reduce(Iterable<Text> athlete_All, NullWritable n, Context context) throws IOException, InterruptedException
         {
        	 Text result = new Text();
        	 for(Text val : athlete_All)
        	 {
        	     String line = val.toString(); String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        	     result.set(fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3] + "," + fields[4] + "," + fields[5] + "," + fields[6]+ "," + fields[7] + "," + fields[8] + "," + fields[9] + "," + fields[10]);
        	 }
        	 if(counter < 10)
   	      	 {
        	     context.write(result,NullWritable.get());
   	      	 }
    	 }
    }
    
    //---------------------------------------------------------------------M A I N---------------------------------------------------------------------
    public static void main(String[] args) throws Exception
    {
      if(args.length != 6)
      {
	  System.err.println("Please provide input and output paths.");
	  System.exit(-1);
      }
      
      Configuration conf1 = new Configuration();
      Job job1 = Job.getInstance(conf1, "CountingMedals");
      job1.setJarByClass(Athletes.class);
      job1.setMapperClass(AMapper.class);
      job1.setCombinerClass(AReducer.class);
      job1.setReducerClass(AReducer.class);
      job1.setMapOutputKeyClass(Text.class);
      job1.setMapOutputValueClass(Text.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job1, new Path(args[1]));
      FileOutputFormat.setOutputPath(job1, new Path(args[2]));
      
      job1.waitForCompletion(true);
      
      Configuration conf2 = new Configuration();
      Job job2 = Job.getInstance(conf2, "SummingMedalsOfAthletesInEachGame");
      job2.setJarByClass(Athletes.class);
      job2.setMapperClass(MMapper.class);
      job2.setCombinerClass(MReducer.class);
      job2.setReducerClass(MReducer.class);
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job2, new Path(args[2]+"/part-r-00000"));
      FileOutputFormat.setOutputPath(job2, new Path(args[3]));
      
      job2.waitForCompletion(true);
      
      Configuration conf3 = new Configuration();
      Job job3 = Job.getInstance(conf3, "SortingAthletes");
      job3.setJarByClass(Athletes.class);
      job3.setMapperClass(SortMapper.class);
      job3.setCombinerClass(SortReducer.class);
      job3.setReducerClass(SortReducer.class);
      job3.setSortComparatorClass(KeyComprator3.class);
      job3.setMapOutputKeyClass(Text.class);
      job3.setMapOutputValueClass(NullWritable.class);
      job3.setOutputKeyClass(Text.class);
      job3.setOutputValueClass(NullWritable.class);
      FileInputFormat.addInputPath(job3, new Path(args[3]+"/part-r-00000"));
      FileOutputFormat.setOutputPath(job3, new Path(args[4]));
      
      job3.waitForCompletion(true);
      
      Configuration conf4 = new Configuration();
      Job job4 = Job.getInstance(conf4, "Top10Athletes");
      job4.setJarByClass(Athletes.class);
      job4.setMapperClass(Top10Mapper.class);
      job4.setCombinerClass(Top10Reducer.class);
      job4.setReducerClass(Top10Reducer.class);
      job4.setSortComparatorClass(KeyComprator4.class);
      job4.setMapOutputKeyClass(Text.class);
      job4.setMapOutputValueClass(NullWritable.class);
      job4.setOutputKeyClass(Text.class);
      job4.setOutputValueClass(NullWritable.class);
      FileInputFormat.addInputPath(job4, new Path(args[4]+"/part-r-00000"));
      FileOutputFormat.setOutputPath(job4, new Path(args[5]));
      
      System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}







