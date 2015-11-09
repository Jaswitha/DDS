package sample.sample1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

public class SpatialRange {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setMaster("local").setAppName("check");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> input = sc.textFile("/home/pavan/workspace/RangeQueryRectangle.csv");
        String window = input.first();
        String [] inputCoordinates = window.split(",");
        
        Double x1 =Double.parseDouble(inputCoordinates[0]);
        Double y1 =Double.parseDouble(inputCoordinates[1]);
        Double x2 =Double.parseDouble(inputCoordinates[2]); 
        Double y2 =Double.parseDouble(inputCoordinates[3]);
        
        Double[] rectangle={x1,y1,x2,y2};
        
        Broadcast<Double[]> broadcastVar = sc.broadcast(rectangle);
    	final Double[] broad = broadcastVar.value();
        
        input = sc.textFile("/home/pavan/workspace/RangeQueryTestData.csv");
        JavaRDD<Integer> SpatialRangeOutput = input.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {

            public Iterable<Integer> call(Iterator<String> t) throws Exception {
            	String inputline="";
            	try{
                
                ArrayList<Integer> outputpoints = new ArrayList<Integer>();
                while(t.hasNext())
                {
                     inputline = t.next();
                    ArrayList<String[]> inputCoordinates = new ArrayList<String[]>();
                    inputCoordinates.add(inputline.split(","));
                    //System.out.println("LOL");
                    //System.out.println(inputline);
                    Double id =Double.parseDouble(inputCoordinates.get(0)[0]);
                    Double x1 =Double.parseDouble(inputCoordinates.get(0)[1]);
                    Double y1 =Double.parseDouble(inputCoordinates.get(0)[2]);
                    //System.out.println("XOXOXO");
                    //System.out.println(x1);
                    if((x1 < broad[2] && x1 > broad[0]) && (y1 > broad[1] && y1 < broad[3]))
                    {
                        outputpoints.add(id.intValue());
                    }
                }
                return outputpoints;
            }
            catch(Exception e)
            {
            	System.out.println(inputline);
            	e.printStackTrace();
            	return null;
            }}
        });
        SpatialRangeOutput.saveAsTextFile("/home/pavan/workspace/spatialrange.txt");
        sc.close();
        
    

	}

}

