package BDPA;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





import java.io.*;
import java.util.*;


public class SimilarityOptimized extends Configured implements Tool {

   
   public static enum COUNTER {
		  COUNT_COMPARAISONS_OPTIMIZED
		  };
		  
		  

   public static void main(String[] args) throws Exception {
	   
      System.out.println(Arrays.toString(args));
      
      
      int res = ToolRunner.run(new Configuration(), new SimilarityOptimized(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));

      // First job 

      Job job = new Job(getConf(), "SimilarityOptimized_part1");
      job.setJarByClass(SimilarityOptimized.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map_inverted.class);
      job.setReducerClass(Reduce_inverted.class);
     

      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // set key-value separator for the input file
      job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
      
      /* set output as csv file */
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job,new Path(args[1]));

      job.waitForCompletion(true);

      // Second job

      Job job2 = new Job(getConf(), "SimilarityOptimized_part2");
	  job2.setJarByClass(SimilarityOptimized.class);

	  job2.setMapperClass(Map.class);
	  job2.setReducerClass(Reduce.class);

	  job2.setOutputKeyClass(Text.class);
	  job2.setOutputValueClass(Text.class);

	  job2.setInputFormatClass(KeyValueTextInputFormat.class);
	  job2.setOutputFormatClass(TextOutputFormat.class);

	  // set key-value separator for the input file
      job2.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
      
      /* set output as csv file */
      job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

	  TextInputFormat.addInputPath(job2, new Path(args[1]));
	  TextOutputFormat.setOutputPath(job2, new Path(args[2]));

	  job2.waitForCompletion(true);

	  // Write counter to file
      FileSystem fs = FileSystem.newInstance(getConf());
      long counter = job2.getCounters().findCounter(COUNTER.COUNT_COMPARAISONS_OPTIMIZED).getValue();
      Path outFile = new Path(new Path(args[2]),"NB_Comp_SimilarityOptimized.txt");
      BufferedWriter writer = new BufferedWriter(
    		  					new OutputStreamWriter(
    		  							fs.create(outFile, true)));
      writer.write(String.valueOf(counter));
      writer.close();
      
      return 0;
   }
   
   public static class Map_inverted extends Mapper<Text, Text,Text,Text> {

   		private Text word = new Text();

   		@Override
   		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {



   			/*  Map first |d| - ceiling(s*|d|) + 1 words of each line 
   				|d| is the number of words in the document
   				s = similarity threshold */
   			
   			if (!value.toString().isEmpty()){
   		    // Get the list of words in the line 
   			
   			Set<String> words_line = new LinkedHashSet<String>();
   			
   			for (String val : value.toString().split(" ")){
   				words_line.add(val);
   			}


			// Compute the number of words to keep 

			int nb_words = new Integer(words_line.size() - (int)Math.ceil(0.8*words_line.size()) +1 );

			// Write the first nb_words in context with the word as key and the line id as value


			int i = 1;
			for (String val : words_line) {       
			    if (i > nb_words) break; 
			    context.write(new Text(val),key);

			    i++;
			}

        }
   		}


   }

   public static class Reduce_inverted extends Reducer<Text, Text, Text, Text> {

  
   	  
   	  @Override
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	  		/* Create the inverted index by regrouping (key,value) from mapper*/


     	 StringBuilder reducedvalue = new StringBuilder();
         HashSet<String> setvalue = new HashSet<String>();

         // Add the values in setvalue 

         for (Text val : values)
         {
          setvalue.add(val.toString());
         }

         // Create the string containing each line id's

         for (String id : setvalue){
         	if (reducedvalue.length() !=0){
         		reducedvalue.append(" ");
         	}

         	reducedvalue.append(id);

         }

         // write the results

         context.write(key,new Text(reducedvalue.toString()));



	}

   }


   
   public static class Map extends Mapper<Text, Text,Text,Text> {
      
      private Text word = new Text();

     /* Initialise one time a hashmap to store key (line id) and the corresponding world*/   
      private static HashMap<String,String> doc_id_contents = new HashMap<String,String>();
     
   
      public Map() throws NumberFormatException, IOException{
     
        /* Default constructor to store pair of line_id content */
       BufferedReader Reader_count = new BufferedReader(
                 new FileReader(
                          new File(
                                  "/home/cloudera/workspace/Assignment2/SimilarityOptimized/input/sample.txt"
                              )));
          
          String line;

          while ((line = Reader_count.readLine()) != null)
          {
              String[] parts = line.split(",", 2);
              if (parts.length >= 2)
              {
                 
                  doc_id_contents.put(parts[0],parts[1]);
              
              } else {
                  System.out.println("ignoring line: " + line);
              }
          }
          Reader_count.close();
       
        }
      


      @Override
      public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

      		  /* Map lines containing the same words together, input here is the output of the job 1
      		     i.e. the inverted index  */
    	  if (!value.toString().isEmpty()){
      		  Set<String> list_ids = new HashSet<String>(Arrays.asList(value.toString().split(" ")));
      		  
      		 
      		  /*
              for (String val : value.toString())
              {
               list_ids.add(val);
              } */
      		  for (String id1 : list_ids){

      		  		int id1_int = Integer.valueOf(id1);

      		  	for (String id2 : list_ids){
            	  	
            	  	int id2_int = Integer.valueOf(id2);

            	  	// we don't want to compare same id and no duplicates, at least for the considered word 
            	  	if (id2_int > id1_int){

            	  		StringBuilder keyBuilder = new StringBuilder();
	                    keyBuilder.append(id1);
	                    keyBuilder.append("@");
	                    keyBuilder.append(id2);
	                    

	                    
	                    context.write(new Text(keyBuilder.toString()), new Text(doc_id_contents.get(id1).toString()));

            	  	}

            	  }


      		  }
    	  }

        }
    }
   
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {

      private static HashMap<String,String> id_contents = new HashMap<String,String>();
     
   
      public Reduce() throws NumberFormatException, IOException{
     
      /* Default constructor to store pair of line_id content */
       BufferedReader Reader_count = new BufferedReader(
                 new FileReader(
                          new File(
                                  "/home/cloudera/workspace/Assignment2/SimilarityOptimized/input/sample.txt"
                              )));
          
          String line;

          while ((line = Reader_count.readLine()) != null)
          {
              String[] parts = line.split(",", 2);
              if (parts.length >= 2)
              {
                 
                  id_contents.put(parts[0],parts[1]);
              
              } else {
                  System.out.println("ignoring line: " + line);
              }
          }
          Reader_count.close();
       
        }
     
 
        public static double JaccardSimilarity(Set<String> set1, Set<String> set2){
        // Method to compute the Jaccard similarity between two hashsets 
            Set<String> intersect = new HashSet<>();
            Set<String> union = new HashSet<>();
            intersect.clear();
            intersect.addAll(set1);
            intersect.retainAll(set2);
            union.clear();
            union.addAll(set1);
            union.addAll(set2);
            return (double)intersect.size()/(double)union.size();
        }
	   
  	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  
    	  
  		  
        
  		 List <String> list_id = new ArrayList<String>(Arrays.asList(key.toString().split("@")));
         String first_id = list_id.get(0);
         
         /* Get the second id's content */
  		 String second_id = list_id.get(1);
         String second_content = id_contents.get(second_id);
         
         
         // Create set for the words in the first document
         Set<String> setvalue_first = new HashSet<String>(); //Arrays.asList(first_content.split(" ")));
         Set<String> temp = new HashSet<String>();
         
         Iterator<Text> iter = values.iterator();
         while (iter.hasNext())
             temp.add(iter.next().toString());
         
         for (String val : temp.iterator().next().split(" ")){

          setvalue_first.add(val);
         } 
         
         // Create set for the words in the second document
         Set<String> setvalue_second = new HashSet<String>(Arrays.asList(second_content.split(" ")));

          
         // Compute Jaccard Similarity 
         double similarity = JaccardSimilarity(setvalue_first,setvalue_second);

         
         // Increment counter of number of comparisons
         context.getCounter(COUNTER.COUNT_COMPARAISONS_OPTIMIZED).increment(1);

         if (similarity >=0.8){
         StringBuilder ListKeyToText = new StringBuilder();
         ListKeyToText.append(first_id);
         ListKeyToText.append("@");
         ListKeyToText.append(second_id);

         
         // write key similarity if enough similar
         context.write(new Text(ListKeyToText.toString()),new Text(String.valueOf(similarity)));
       }
      }
      
   }
}
