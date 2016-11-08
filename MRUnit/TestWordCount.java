package solution;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestWordCount {

 
  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    
    WordMapper mapper = new WordMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
    mapDriver.setMapper(mapper);

   
    SumReducer reducer = new SumReducer();
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
    reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

 
  @Test
  public void testMapper() {

    /*
     * For this test, the mapper's input will be "1 London London Paris" 
     */
    mapDriver.withInput(new LongWritable(1), new Text("London London Paris"));

    /*
     * The expected output is "London 1", "London 1", and "Paris 1".
     */
    mapDriver.withOutput(new Text("London"), new IntWritable(1));
    mapDriver.withOutput(new Text("London"), new IntWritable(1));
    mapDriver.withOutput(new Text("Paris"), new IntWritable(1));

    /*
     * Run the test.
     */
    mapDriver.runTest();
  }

  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {

    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));

    /*
     * For this test, the reducer's input will be "London 1 1".
     */
    reduceDriver.withInput(new Text("London"), values);

    /*
     * The expected output is "London 2"
     */
    reduceDriver.withOutput(new Text("London"), new IntWritable(2));

    /*
     * Run the test.
     */
    reduceDriver.runTest();
  }

  /*
   * Test the mapper and reducer working together.
   */
  @Test
  public void testMapReduce() {

    /*
     * For this test, the mapper's input will be "1 London London Paris" 
     */
    mapReduceDriver.withInput(new LongWritable(1), new Text("London London Paris"));

    /*
     * The expected output (from the reducer) is "London 2", "Paris 1". 
     */
    mapReduceDriver.addOutput(new Text("London"), new IntWritable(2));
    mapReduceDriver.addOutput(new Text("Paris"), new IntWritable(1));

    /*
     * Run the test.
     */
    mapReduceDriver.runTest();
  }
}

