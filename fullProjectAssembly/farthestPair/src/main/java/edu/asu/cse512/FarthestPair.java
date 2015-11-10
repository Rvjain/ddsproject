package edu.asu.cse512;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * Hello world!
 *
 */
public class FarthestPair 
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
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> lines = convexHull.getConvexHull(context,args[0]);
		JavaRDD<Coordinate> farthestPoints = lines.mapPartitions(new globalFarthest());
        JavaRDD<String> coordsString = farthestPoints.mapPartitions(new CoordinateSave());
		deleteIfExist(args[1]);
		coordsString.saveAsTextFile(args[1]);
		context.close();
        
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


class globalFarthest implements FlatMapFunction<Iterator<String>, Coordinate>, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Iterable<Coordinate> call(Iterator<String> convexHull)
			throws Exception {
		double max = 0.0;
		List<Coordinate> convexList = new ArrayList<Coordinate>();
		List<Coordinate> finalResult = new ArrayList<Coordinate>();
		while(convexHull.hasNext())
		{
			String point = convexHull.next();
			String[] coords = point.split(",");
			Coordinate coord = new Coordinate(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
			convexList.add(coord);
		}
		for(Coordinate primCoord: convexList){
			for(Coordinate secCoord: convexList){
				if(primCoord.distance(secCoord) > max){
					finalResult = new ArrayList<Coordinate>();
					max = primCoord.distance(secCoord);
					finalResult.add(primCoord);
					finalResult.add(secCoord);
				}
			}
		}
		
		return finalResult;
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
