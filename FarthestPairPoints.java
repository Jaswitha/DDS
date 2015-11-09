package sample.sample1;

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

public class FarthestPairPoints {

	public static void main(String args[]) throws IOException
	{
	    Date date1 = new Date();
	    System.out.println(date1.toString());
		SparkConf conf = new SparkConf()
	            .setMaster("local")
	            .setAppName("check");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("/home/pavan/workspace/FarthestPairTestData.csv");
		JavaRDD<Coordinate> localconvexpoints = input.mapPartitions(new FlatMapFunction<Iterator<String>,Coordinate>(){
			  private static final long serialVersionUID = 1L;
			  public Iterable<Coordinate> call(Iterator<String> s) throws Exception {
				
				ArrayList<Coordinate> inputpoints = new ArrayList<Coordinate>();  
				ArrayList<Coordinate> outputpoints = new ArrayList<Coordinate>(); 
				ArrayList<String[]> inputCoordinates = new ArrayList<String[]>();
				GeometryFactory gf  = new GeometryFactory();
				
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
			private static final long serialVersionUID = 1L;
			public Iterable<Coordinate> call(Iterator<Coordinate> t) throws Exception {
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

		List<Coordinate> GlobalConvexHullPoints = globalConvexpoints.collect();
		Coordinate farthest_a, farthest_b;
		int totalConvexHullPoints = GlobalConvexHullPoints.size();
		
		double farthestDistance = 0;
		farthest_a = GlobalConvexHullPoints.get(0);
		farthest_b = GlobalConvexHullPoints.get(1);
		
		for (int i = 0;i<totalConvexHullPoints-1;i++)
		{
			for (int j = i+1;j<totalConvexHullPoints;j++)
			{
				double localDistance = Math.sqrt(Math.pow(GlobalConvexHullPoints.get(i).x-GlobalConvexHullPoints.get(j).x,2)+(Math.pow(GlobalConvexHullPoints.get(i).y-GlobalConvexHullPoints.get(j).y,2)));
				if (localDistance > farthestDistance)
				{
					farthestDistance = localDistance;
					farthest_a = GlobalConvexHullPoints.get(i);
					farthest_b = GlobalConvexHullPoints.get(j);
					//System.out.println("farthestDistance = "+farthestDistance+", a = "+farthest_a+", b = "+farthest_b);
				}
			}
		}
		
		ArrayList<Coordinate> farthestPoints = new ArrayList<Coordinate>();
		farthestPoints.add(farthest_a);
		farthestPoints.add(farthest_b);
		//System.out.println("Final farthestDistance = "+farthestDistance+", a = "+farthest_a+", b = "+farthest_b);
		JavaRDD<Coordinate>globalFathestPoints = sc.parallelize(farthestPoints);
		globalFathestPoints.saveAsTextFile("/home/pavan/workspace/farthestpairpoints.txt");
		sc.close();
		Date date2 = new Date();
	    System.out.println(date2.toString());
		}
}
