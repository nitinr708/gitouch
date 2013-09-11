package wpmcn.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * See the <a href="https://cwiki.apache.org/confluence/display/MRUNIT/Index">MRUnit Wiki</a> for more information.
 */
public class WordCountTest {
   MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
   MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
   ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
   
   LongWritable oneLongWritable = new LongWritable(1);
   LongWritable twoLongWritables = new LongWritable(2);
   Text textWords = new Text("cat cat dog");
   Text catWord = new Text("cat");
   Text dogWord = new Text("dog");
   	
   @Before
   public void setUp() {
      WordCount.WordCountMapper mapper = new WordCount.WordCountMapper();
      WordCount.WordCountReducer reducer = new WordCount.WordCountReducer();
      mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
      mapDriver.setMapper(mapper);
      reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
      reduceDriver.setReducer(reducer);
      mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
      mapReduceDriver.setMapper(mapper);
      mapReduceDriver.setReducer(reducer);
   }

   @Test
   public void testMapper() throws Exception {
      mapDriver.withInput(oneLongWritable, textWords);
      mapDriver.withOutput(catWord, oneLongWritable);
      mapDriver.withOutput(catWord, oneLongWritable);      
      mapDriver.withOutput(dogWord, oneLongWritable);
      System.out.println("");
      System.out.println("---------Mapper Input & Output -----------");
      System.out.println("Mapper input: ("+oneLongWritable+", "+textWords+")");
      System.out.println("Mapper output: "+mapDriver.run());
      mapDriver.runTest();
   }

   @Test
   public void testReducer1() throws IOException {
      List<LongWritable> arrTwoLongWritables = new ArrayList<LongWritable>();
      arrTwoLongWritables.add(oneLongWritable);
      arrTwoLongWritables.add(oneLongWritable);
      reduceDriver.withInput(catWord, arrTwoLongWritables);
      reduceDriver.withOutput(catWord, twoLongWritables);
      System.out.println("");
      System.out.println("---------Reducer Split 1 Input & Output -----------");
      System.out.println("reducer input is: ("+catWord+", "+arrTwoLongWritables+")");
      System.out.println("reducer_output is: "+reduceDriver.run());
      reduceDriver.runTest();
   }
   
   @Test
   public void testReducer2() throws IOException {
	     List<LongWritable> arrOneLongWritable = new ArrayList<LongWritable>();
	     arrOneLongWritable.add(oneLongWritable);
	 
	      reduceDriver.withInput(dogWord, arrOneLongWritable);
	      reduceDriver.withOutput(dogWord, oneLongWritable);
	      System.out.println("");
	      System.out.println("---------Reducer Split 2 Input & Output -----------");
	      System.out.println("reducer input is: ("+dogWord+", "+oneLongWritable+")");
	      System.out.println("reducer_output is: "+reduceDriver.run()); 
	      reduceDriver.runTest();
   }

   @Test
   public void testMapReduce() throws IOException {
      mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
      mapReduceDriver.addOutput(new Text("cat"), new LongWritable(2));
      mapReduceDriver.addOutput(new Text("dog"), new LongWritable(1));
      System.out.println("");
      System.out.println("---------MR Job Input & Output -----------");
      System.out.println("MapReduce Job Input is: ("+oneLongWritable+", "+textWords+")");
      System.out.println("MapReduce Job Output is: "+mapReduceDriver.run());
      mapReduceDriver.runTest();
   }
}
