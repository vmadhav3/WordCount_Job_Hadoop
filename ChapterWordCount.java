//Importing all the packages required
      
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class ChapterWordCount {

// Start of the Mapper Method to read Chapter input and split it into (Word,ChapterNum)<Key,Value> pairs

public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
	private Text word = new Text();

// String with all the special characters    	

	private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"'%]";
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

// Replace all the special characters and delete numerics and convert to lower case

    	String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ").replaceAll("\\d","");

// Split the cleaned line into tokens using StringTokenizer    	

	StringTokenizer tokenizer = new StringTokenizer(cleanLine);

// Iterate through the tokens and output the <Key,Value> pairs to the reducers

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word,key);
       }
    	
    	}
    }

// Method to check if a number in an array is unique

public static boolean isUnique(int[] array, int num) {
     for (int i = 0; i < array.length; i++) {
         if (array[i] == num) {
             return false;
         }
     }
     return true;
 }

 
// Convert the given array to an array with unique values without duplicates and Return it
 
public static int[] toUniqueArray(int[] array) {
     int[] temp = new int[array.length];

     for (int i = 0; i < temp.length; i++) {
         temp[i] = -1; 
     }
     int counter = 0;

     for (int i = 0; i < array.length; i++) {
         if (isUnique(temp, array[i]))
             temp[counter++] = array[i];
     }
     int[] uniqueArray = new int[counter];

     System.arraycopy(temp, 0, uniqueArray, 0, uniqueArray.length);

     return uniqueArray;
 }

// Start of the Reducer Method to read Mapper Input and Check for Common Words across all the chapters and write it to the output file.

public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	
    public void reduce(Text key, Iterable<LongWritable> values, Context context) 
     throws IOException, InterruptedException {
        int Count;
        int UniqueCount;
        String Str="";

	// Condition to eliminate words (such as a, an, the etc.,) which are less than 3 letters

        if (key.getLength()>3)
        {

	// Loop to iterate through the values sent by the Mapper after splitting		        
			for (LongWritable val : values) {
		      
		// Concatinating all the values to create one string to form an array later by splitting it

		      		if (Str!="")
		       		   Str=Str + "," + val.toString();
		                else
		        	   Str=val.toString();
		       }			
		// Split the string to form an array

		        String[] strArray = Str.split(",");
		        int[] intArray = new int[strArray.length];
		// Store the count of the word into a variable to print it to the output if the word is present in all the chapters		        
			Count=strArray.length;
		        
		// Convert the chapter numbers into string array
			for(int i = 0; i < strArray.length; i++) {
		            intArray[i] = Integer.parseInt(strArray[i]);
		        }
		
		// Create an array with unique elements to check if the word is present in all the chapters. 
		// Use the toUniqueArray method to get the unique array. 
		        
			int[] UniqueArr=toUniqueArray(intArray);
		// We can check if a word is present in all the chapters by checking the length of the unique array created. 
		//Print to the output if present in all chapters.
		        if (UniqueArr.length==12||UniqueArr.length==13)
		        context.write(key, new LongWritable(Count));
        }
   }
}
        
 public static void main(String[] args) throws Exception {

// Regular Expression to identify chapter beginnings in the book	
	String regex ="^CHAPTER\\s+.*$";
//Create new Configuration object
	Configuration conf = new Configuration();
// Set the delimiter regex to split the book into chapters and pass it as input to the mappers 	
	conf.set("record.delimiter.regex", regex);	
// Create new Job named ChapterWordCount 
	Job job = new Job(conf, "ChapterWordCount");
// Set Output Key and Value Types
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(LongWritable.class);
// Set Mapper and Reducer Classes
    	job.setMapperClass(Map.class);
    	job.setReducerClass(Reduce.class);
// As we are implementing our own InputFormat we have to specify it in the driver class by setting the setInputFormatClass property of job object.
    	job.setInputFormatClass(PatternInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
// code to take input and output paths from arguments while running
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
// Run the job and wit for completion
    	job.waitForCompletion(true);
 }
        
}

