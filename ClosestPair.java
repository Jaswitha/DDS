
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Iterator;
import java.util.Date;

import com.vividsolutions.jts.geom.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class ClosestPair {

    public static void main(String args[]) throws IOException
    {
      Date date1 = new Date();
      System.out.println(date1.toString());
        SparkConf conf = new SparkConf()
              .setMaster("local")
              .setAppName("check");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("/home/pavan/workspace/aid.csv");
        JavaRDD<Coordinate> localconvexpoints = input.mapPartitions(new FlatMapFunction<Iterator<String>,Coordinate>(){
            private static final long serialVersionUID = 1L;
            public Iterable<Coordinate> call(Iterator<String> s) throws Exception {
                
                ArrayList<Coordinate> inputpoints = new ArrayList<Coordinate>(); 
                ArrayList<Coordinate> outputpoints = new ArrayList<Coordinate>();
                ArrayList<String[]> inputCoordinates = new ArrayList<String[]>();
                
                while(s.hasNext())
                {
                            
                String inputline = s.next();
                inputCoordinates.add(inputline.split(","));
                Double x1 =Double.parseDouble(inputCoordinates.get(0)[0]);
                Double y1 =Double.parseDouble(inputCoordinates.get(0)[1]);
            
                Coordinate coord = new Coordinate(x1,y1);
                inputpoints.add(coord);
                inputCoordinates.remove(0);
                }
                Coordinate[] inputpts= new Coordinate[inputpoints.size()];
                inputpts= inputpoints.toArray(new Coordinate[inputpoints.size()]);
                for(int i=0;i<inputpts.length;i++)
    			{
    				outputpoints.add(inputpts[i]);
    			
    			}return outputpoints;
                 
            }
        });
        
        JavaRDD<Coordinate>globalConvexpoints=localconvexpoints.mapPartitions(new FlatMapFunction<Iterator<Coordinate>,Coordinate>(){
            private static final long serialVersionUID = 1L;
            public Iterable<Coordinate> call(Iterator<Coordinate> t) throws Exception {
                ArrayList<Coordinate> inputpoints = new ArrayList<Coordinate>();  
                ArrayList<Coordinate> outputpoints = new ArrayList<Coordinate>(); 
                while(t.hasNext())
                {
                    inputpoints.add(t.next());
                }
                
//Coordinate[] inputpts=  inputpoints.toArray(new Coordinate[inputpoints.size()]);
                Coordinate closest_a, closest_b;
                int points = inputpoints.size();
                
                
                closest_a = inputpoints.get(0);
                closest_b = inputpoints.get(1);
                double leastDistance = Math.sqrt(Math.pow(inputpoints.get(0).x-inputpoints.get(1).x, 2))+(Math.pow(inputpoints.get(0).y - inputpoints.get(1).y ,2));
                System.out.println(points);
                for (int i = 0;i<points-1;i++)
                {
                    for (int j = i+1;j<points;j++)
                    {
                        double localDistance = Math.sqrt(Math.pow(inputpoints.get(i).x-inputpoints.get(j).x, 2))+(Math.pow(inputpoints.get(i).y - inputpoints.get(j).y ,2));
                        if (localDistance < leastDistance)
                        {
                            leastDistance = localDistance;
                            closest_a = inputpoints.get(i);
                            closest_b = inputpoints.get(j);
                            System.out.println("leastDistance = "+leastDistance+", a = "+closest_a+", b = "+closest_b);
                        }
                    }
                }
                outputpoints.add(closest_a);
                outputpoints.add(closest_b);
                return outputpoints;
             
            }
            
});
        globalConvexpoints.saveAsTextFile("/home/pavan/workspace/closestpairpoints.txt");
        
        }
}


