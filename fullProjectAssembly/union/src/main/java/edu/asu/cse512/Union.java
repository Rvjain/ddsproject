package edu.asu.cse512;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

/**
 * Hello world!
 *
 */

public class Union 
{
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	*/
    public static void main( String[] args )
    {
    	SparkConf conf = new SparkConf().setAppName("App");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> linesLocal = sc.textFile(args[0]);
		JavaRDD<Polygon> MappedPolygons = linesLocal.mapPartitions(new PolygonCoordinates());
		JavaRDD<Polygon> MappedGeometries = MappedPolygons.mapPartitions(new PolygonUnion());
		JavaRDD<Polygon> ReduceList = MappedGeometries.coalesce(1);
		JavaRDD<Polygon> FinalList = ReduceList.mapPartitions(new PolygonUnion());
		
		JavaRDD<Coordinate> coords =  FinalList.mapPartitions(new PolyToCoord());
		deleteIfExist(args[1]);
		coords.distinct().sortBy( new Function<Coordinate,Coordinate>() {

			public Coordinate call(Coordinate str) throws Exception {
				// TODO Auto-generated method stub
				return str;
			}
			}, true, 1 ).mapPartitions(new CoordinateSave()).saveAsTextFile(args[1]);;
		
    }
    
    public static void deleteIfExist(String key) {
		URI uri = URI.create(key);
		try {
			FileSystem fs = FileSystem.get(uri, new Configuration());
			if (fs.exists(new Path(uri)))
				fs.delete(new Path(uri), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class PolygonCoordinates implements FlatMapFunction<Iterator<String>, Polygon>, Serializable{
	/**
 * 
 */
private static final long serialVersionUID = 1L;

	public Iterable<Polygon> call(Iterator<String> text) throws Exception {
		List<Polygon> polygonList = new ArrayList<Polygon>();
		while(text.hasNext())
		{
			String line = text.next();
			String[] CoordList = line.split(",");
			Double x1 = Double.parseDouble(CoordList[0]);
			Double y1 = Double.parseDouble(CoordList[1]);
			Double x2 = Double.parseDouble(CoordList[2]);
			Double y2 = Double.parseDouble(CoordList[3]);
			Coordinate[] coordinates = new Coordinate[5];
			coordinates[0] = new Coordinate(x1,y1);
			coordinates[1] = new Coordinate(x1,y2);
			coordinates[2] = new Coordinate(x2,y2);
			coordinates[3] = new Coordinate(x2,y1);
			coordinates[4] = new Coordinate(x1,y1);
			// Ref: http://stackoverflow.com/questions/6570017/how-to-create-a-polygon-in-jts-when-we-have-list-of-coordinate
			GeometryFactory fact = new GeometryFactory();
			LinearRing linear = new GeometryFactory().createLinearRing(coordinates);
			Polygon poly = new Polygon(linear, null, fact);
			polygonList.add(poly);
		}
		return polygonList;
}
}


class PolygonUnion implements FlatMapFunction<Iterator<Polygon>, Polygon>, Serializable{

/**
 * 
 */
private static final long serialVersionUID = 1L;

public Iterable<Polygon> call(Iterator<Polygon> geos) throws Exception {
	List<Polygon> unionPolygon = IteratorUtils.toList(geos);
	CascadedPolygonUnion unionPoly = new CascadedPolygonUnion(unionPolygon);
	List<Polygon> finalPolygon = new ArrayList<Polygon>();
	finalPolygon.add((Polygon) unionPoly.union());
	return finalPolygon;
}
}
class PolyToCoord implements FlatMapFunction<Iterator<Polygon>, Coordinate>, Serializable{
	private static final long serialVersionUID = 1L;

	public Iterable<Coordinate> call(Iterator<Polygon> polygons) throws Exception {
		List<Coordinate> coords = new ArrayList<Coordinate>();
		while(polygons.hasNext()){
			Polygon currPolygon = polygons.next();
			Coordinate[] coordinates = currPolygon.getCoordinates();
			coords.addAll(Arrays.asList(coordinates));
//			for (Coordinate coord: coordinates){
//				String coordString = coord.x + "," + coord.y;
//				coords.add(coordString);
//			}
		}
		return coords;
	}
}
class CoordinateSave implements FlatMapFunction<Iterator<Coordinate>, String>, Serializable{
	public Iterable<String> call(Iterator<Coordinate> coordinates) throws Exception {
		List<String> coords = new ArrayList<String>();
		while(coordinates.hasNext()){
			Coordinate coord = coordinates.next();
			String coordString = coord.x + "," + coord.y;
			coords.add(coordString);
		}
		return coords;
	}
}
