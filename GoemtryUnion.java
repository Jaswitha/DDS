package sample.sample1;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;
public class GoemtryUnion {
	public static void main(String args[]) throws IOException
	{
	
		SparkConf conf = new SparkConf()
	            .setMaster("local")
	            .setAppName("check");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile("");
		
		JavaRDD<Geometry> points = input.flatMap(new FlatMapFunction<Iterable<String>,Geometry>(){
			
				ArrayList<Geometry> outputPolygon = new ArrayList<Geometry>();
				ArrayList<Geometry> ActivePolygon = new ArrayList<Geometry>();
				ArrayList<String[]> inputCoordinates = new ArrayList<String[]>();
				GeometryFactory gf  = new GeometryFactory();
				public Iterable<Geometry> call(Iterable<String> s) throws Exception{
				Iterator it = s.iterator();
				while(it.hasNext())
				{
					String inputline = it.next().toString();
					inputCoordinates.add(inputline.split(","));
					double x1 =Double.parseDouble(inputCoordinates.get(0)[0]);
					double y1 =Double.parseDouble(inputCoordinates.get(0)[1]);
					double x2 =Double.parseDouble(inputCoordinates.get(0)[2]); 
					double y2 =Double.parseDouble(inputCoordinates.get(0)[3]);
					Coordinate c1 = new Coordinate(x1,y1);
					Coordinate c2 = new Coordinate(x2,y2);
					Coordinate[] polygonPoints = {c1,c2};
					Geometry polygon = gf.createPolygon(polygonPoints);
					ActivePolygon.add(polygon);
				}
				 CascadedPolygonUnion polygonunion = new  CascadedPolygonUnion(ActivePolygon);
				Geometry finalOutputpolygon =polygonunion.union();
				 int no_polygon= finalOutputpolygon.getNumGeometries();
				 for(int i=0;i<no_polygon;i++)
				 {
					 Geometry g =finalOutputpolygon.getGeometryN(i);
					 outputPolygon.add(g);
				 }
				 return outputPolygon;
			  }
				  
			  });
		
		
}
}
