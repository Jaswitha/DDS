package sample.sample1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.vividsolutions.jts.geom.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class ConvexHull {
	private static final String String = null;

	public static void main(String args[]) throws IOException
	{
	
		SparkConf conf = new SparkConf()
	            .setMaster("local")
	            .setAppName("check");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("/home/jaswitha/InputData/aid.csv");
		JavaRDD<Coordinate> localconvexpoints = input.mapPartitions(new FlatMapFunction<Iterator<String>,Coordinate>(){
			  public Iterable<Coordinate> call(Iterator<String> s) throws Exception {
				
				ArrayList<Coordinate> inputpoints = new ArrayList<Coordinate>();  
				ArrayList<Coordinate> outputpoints = new ArrayList<Coordinate>(); 
				ArrayList<String[]> inputCoordinates = new ArrayList<String[]>();
				GeometryFactory gf  = new GeometryFactory();
				//Coordinate c= new Coordinate(5,4);
				
				while(s.hasNext())
				{
							
				String inputline = s.next();
				inputCoordinates.add(inputline.split(","));
				Double x1 =Double.parseDouble(inputCoordinates.get(0)[0]);
				Double y1 =Double.parseDouble(inputCoordinates.get(0)[1]);
			
				Coordinate coord = new Coordinate(x1,y1);
				inputpoints.add(coord);
				inputCoordinates.remove(0);
				System.out.println("x1"+x1);
				System.out.println("y1"+y1);
				
				}
				System.out.println("inputpoints"+inputpoints);
				Coordinate[] inputpts= new Coordinate[inputpoints.size()];
				inputpts= inputpoints.toArray(new Coordinate[inputpoints.size()]);
				com.vividsolutions.jts.algorithm.ConvexHull convex=  new com.vividsolutions.jts.algorithm.ConvexHull(inputpts, gf);
				Geometry convexHullPolygon = convex.getConvexHull();
				
			Coordinate[]convexHullPoints=	convexHullPolygon.getCoordinates();
			for(int i=0;i<convexHullPoints.length;i++)
			{
				outputpoints.add(convexHullPoints[i]);
			
			}return outputpoints;
				  
			  }
		});
		
		JavaRDD<Coordinate>globalConvexpoints=localconvexpoints.mapPartitions(new FlatMapFunction<Iterator<Coordinate>,Coordinate>(){

			public Iterable<Coordinate> call(Iterator<Coordinate> t) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Coordinate> inputpoints = new ArrayList<Coordinate>();  
				ArrayList<Coordinate> outputpoints = new ArrayList<Coordinate>(); 
				GeometryFactory geomfactory  = new GeometryFactory();
				while(t.hasNext())
				{
					inputpoints.add(t.next());
				}
				Coordinate[] inputpts=  inputpoints.toArray(new Coordinate[inputpoints.size()]);
				com.vividsolutions.jts.algorithm.ConvexHull convex= new com.vividsolutions.jts.algorithm.ConvexHull(inputpts, geomfactory);
				Geometry convexHullPolygon = convex.getConvexHull();
				
			Coordinate[]convexHullPoints=	convexHullPolygon.getCoordinates();
			for(int i=0;i<convexHullPoints.length;i++)
			{
				outputpoints.add(convexHullPoints[i]);
			
			}return outputpoints;
				  
		
			  
			}
			 
			

		
		
	
});
		globalConvexpoints.saveAsTextFile("/home/jaswitha/Downloads/outputconex.txt");	
		}
	
}
