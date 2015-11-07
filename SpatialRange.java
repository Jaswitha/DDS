package sample.sample1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.geom.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

    

public class SpatialRange
{
    private static final String String = null;

    public static void main(String args[]) throws IOException
    {
    
        SparkConf conf = new SparkConf().setMaster("local").setAppName("check");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> input = sc.textFile(args[0]);
        String window = input.first();
        String [] inputCoordinates=window.split(",");
        
        Double x1 =Double.parseDouble(inputCoordinates[0]);
        Double y1 =Double.parseDouble(inputCoordinates[1]);
        Double x2 =Double.parseDouble(inputCoordinates[2]); 
        Double y2 =Double.parseDouble(inputCoordinates[3]);
        
        Double[] rectangle={x1,y1,x2,y2};
        
        Broadcast<Double[]> broadcastVar = sc.broadcast(window);
    	final Double[] broad = broadcastVar.value();
        
        input = sc.textFile(args[1]);
        JavaRDD<Double> SpatialRangeOutput = input.mapPartitions(new FlatMapFunction<Iterator<String>, Double>() {

            public Iterable<Double> call(Iterator<String> t) throws Exception {
                ArrayList<String[]> inputCoordinates = new ArrayList<String[]>();
                ArrayList<Double> outputpoints = new ArrayList<Double>();
                while(t.hasNext())
                {
                    String inputline = t.next();
                    inputCoordinates.add(inputline.split(","));
                    Double id =Double.parseDouble(inputCoordinates.get(0)[0]);
                    Double x1 =Double.parseDouble(inputCoordinates.get(0)[1]);
                    Double y1 =Double.parseDouble(inputCoordinates.get(0)[2]);
                    if((x1 < broad[2] && x1 > broad[0]) && (y1 > broad[1] && y1 < broad[3]))
                    {
                        outputpoints.add(id);
                    }
                }
                return outputpoints;
            }
        });
        SpatialRangeOutput.saveAsTextFile(args[2]);
        sc.close();
    }
}
