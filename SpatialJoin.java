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

public class SpatialJoin
{
    public static void main(String args[]) throws IOException
    {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("check");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("/home/pavan/workspace/aid.csv");
        List<String> input2 = input.collect();
        String[] input2String = input2.toArray(new String[0]);
        
        Broadcast<String[]> broadcastVar = sc.broadcast(input2String);
    	final String[] broad = broadcastVar.value();
        
        input = sc.textFile("/home/pavan/workspace/bid.csv");
        JavaRDD<String> SpatialJoinOutput = input.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            
            public Iterable<String> call(Iterator<String> t) throws Exception {
                ArrayList<String> outputpoints = new ArrayList<String>();
                while(t.hasNext())
                {
                    String outputline = null;
                    String inputline = t.next();
                    ArrayList<String[]> inputCoordinates = new ArrayList<String[]>();
                    inputCoordinates.add(inputline.split(","));
                    Double bid =Double.parseDouble(inputCoordinates.get(0)[0]);
                    Double x1 =Double.parseDouble(inputCoordinates.get(0)[1]);
                    Double y1 =Double.parseDouble(inputCoordinates.get(0)[2]);
                    Double x2 =Double.parseDouble(inputCoordinates.get(0)[3]);
                    Double y2 =Double.parseDouble(inputCoordinates.get(0)[4]);
                    
                    for(String part: broad)
                    {
                        String str[] = part.split(",");
                        
                        if(str.length == 5)
                        {
                        	Double aid = Double.parseDouble(str[0]);
                        	Double p1 = Double.parseDouble(str[1]);
                        	Double q1 = Double.parseDouble(str[2]);
                        	Double p2 = Double.parseDouble(str[3]);
                        	Double q2 = Double.parseDouble(str[4]);

                            if((Math.max(x1, x2) >= Math.max(p1, p2)) && (Math.max(y1, y2) >= Math.max(q1, q2)) && (Math.min(x1, x2) <= Math.min(p1, p2)) && (Math.min(y1, y2) <= Math.min(q1, q2)))
                            {
                                if(outputline == null) outputline = Integer.toString(aid.intValue());
                                else outputline += "," + Integer.toString(aid.intValue());
                            }
                        }
                        
                        else if(str.length == 3)
                        {
                            Double aid = Double.parseDouble(str[0]);
                            Double p1 = Double.parseDouble(str[1]);
                            Double q1 = Double.parseDouble(str[2]);
                            
                            if((p1 <= x2 && p1 >= x1) && (q1 >= y1 && q1 <= y2))
                            {
                                if(outputline == null) outputline = Integer.toString(aid.intValue());
                                else outputline += "," + Integer.toString(aid.intValue());
                            }
                        }
                    }
                    outputpoints.add(Integer.toString(bid.intValue()) +"," +outputline);
                }
                return outputpoints;
            }
        });
        
        SpatialJoinOutput.saveAsTextFile("/home/pavan/workspace/spatialjoin.txt");
    }
}
