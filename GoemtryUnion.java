package sample.sample1;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

import scala.Serializable;
public class GoemtryUnion {
	public static void main(String args[]) throws IOException
	{
	
		SparkConf conf = new SparkConf()
	            .setMaster("local")
	            .setAppName("check");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile("/home/pavan/workspace/UnionQueryTestData.csv");
		JavaRDD<Geometry>localUnion = input.mapPartitions(new FlatMapFunction<Iterator<String>,Geometry>(){
			
				
				
				public Iterable<Geometry> call(Iterator<String> s) throws Exception{
					ArrayList<Geometry> outputPolygon = new ArrayList<Geometry>();
					ArrayList<Geometry> ActivePolygon = new ArrayList<Geometry>();
					ArrayList<String[]> inputCoordinates = new ArrayList<String[]>();
					GeometryFactory gf  = new GeometryFactory();
				//Iterator<String> it = s;
					int k=0;
				while(s.hasNext())
				{
					String inputline = s.next();
					inputCoordinates.add(inputline.split(","));
					
					Double x1 =Double.parseDouble(inputCoordinates.get(0)[0]);
					Double y1 =Double.parseDouble(inputCoordinates.get(0)[1]);
					Double x2 =Double.parseDouble(inputCoordinates.get(0)[2]); 
					Double y2 =Double.parseDouble(inputCoordinates.get(0)[3]);
					System.out.println("x1"+x1);
					System.out.println("y1"+y1);
					System.out.println("x2"+x2);
					System.out.println("y2"+y2);
					Coordinate c1 = new Coordinate(x1,y1);
					Coordinate c2 = new Coordinate(x2,y2);
					Coordinate c3 = new Coordinate(x2,y1);
					Coordinate c4 = new Coordinate(x1,y2);
					if(x1==x2 || y1==y2)
					{
						continue;
					}
					
					Coordinate[] polygonPoints = {c1,c3,c2,c4,c1};
					Geometry polygon = gf.createPolygon(polygonPoints);
					System.out.println(polygon);
					System.out.println(polygon.isValid());
					System.out.println(polygon.isSimple());
					if(polygon.isValid())
						
					ActivePolygon.add(polygon);
					
					
					inputCoordinates.remove(0);
				}
				System.out.println("active polygon"+ActivePolygon);
				
				 
				 
				CascadedPolygonUnion polygonunion = new  CascadedPolygonUnion(ActivePolygon);
				    System.out.println("polygonunion"+polygonunion);
				Geometry finalOutputpolygon = polygonunion.union();
				 int no_polygon= finalOutputpolygon.getNumGeometries();
				 for(int i=0;i<no_polygon;i++)
				 {
					 Geometry g =finalOutputpolygon.getGeometryN(i);
					 outputPolygon.add(g);
				 }
				 return outputPolygon;
			  }

			
				  			
			  });
		JavaRDD<Geometry> polygon = localUnion.repartition(1);
		
			JavaRDD<Geometry> globalUnion = polygon.mapPartitions(new FlatMapFunction<Iterator<Geometry>,Geometry>(){
			public Iterable<Geometry> call(Iterator<Geometry> s) throws Exception{
			ArrayList<Geometry> outputPolygon = new ArrayList<Geometry>();
			ArrayList<Geometry> inputPolygon = new ArrayList<Geometry>();
			Iterator it = s;
			while(it.hasNext())
			{
				inputPolygon.add((Geometry) it.next());
			}
			
				
			 	CascadedPolygonUnion polygonunion = new  CascadedPolygonUnion(inputPolygon);
			 	Object finalOutputpolygon =polygonunion.union();
				 int no_polygon= ((Geometry)finalOutputpolygon).getNumGeometries();
				 for(int i=0;i<no_polygon;i++)
				 {
					 Geometry g =((Geometry)finalOutputpolygon).getGeometryN(i);
					 outputPolygon.add(g);
				 }
				 return outputPolygon;
			}

			
				
		});
		
			globalUnion.saveAsTextFile("/home/pavan/workspace/outputunion.txt");
				
		
}

	
	
	
}
