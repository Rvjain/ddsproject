package edu.asu.cse512;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

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
		JavaRDD<String> lines = getConvexHull(context,args[0]);
		JavaRDD<Coordinate> farthestPoints = lines.mapPartitions(new globalFarthest());
        JavaRDD<String> coordsString = farthestPoints.mapPartitions(new CoordinateSave());
		deleteIfExist(args[1]);
		coordsString.sortBy( new Function<String,String>() {

			public String call(String str) throws Exception {
				// TODO Auto-generated method stub
				return str;
			}
			}, true, 1 ).saveAsTextFile(args[1]);

		context.close();
        
    }
    public static JavaRDD<String> getConvexHull(JavaSparkContext context, String filePath) {
		JavaRDD<String> points = context.textFile(filePath);
		JavaRDD<Coordinate> workerConvexHull = getWorkerHull(points);
		workerConvexHull = workerConvexHull.coalesce(1);
		JavaRDD<String> masterConvexHull = getMasterHull(workerConvexHull);
		return masterConvexHull;
	}
	
	public static JavaRDD<String> getMasterHull(JavaRDD<Coordinate> workerConvexHull) {
		JavaRDD<String> masterHull = workerConvexHull.mapPartitions(new FlatMapFunction<Iterator<Coordinate>, String>() {

			private static final long serialVersionUID = 1L;

			public Iterable<String> call(Iterator<Coordinate> coordinate) throws Exception {
				List<Coordinate> masterConvexHull = new ArrayList<Coordinate>();
				while(coordinate.hasNext()){
					Coordinate c =coordinate.next();
					masterConvexHull.add(c);
				}
				ConvexHull convexhull = new ConvexHull(masterConvexHull.toArray(new Coordinate[masterConvexHull.size()]), new GeometryFactory());
				Coordinate []arrayConvexHull = convexhull.getConvexHull().getCoordinates();
				ArrayList<String> result = new ArrayList<String>();
				for(int i=0;i<arrayConvexHull.length;i++){
					String s = arrayConvexHull[i].x+", "+arrayConvexHull[i].y;
					result.add(s);
				}
				return result;
			}
		});
		return masterHull;
	}

	public static JavaRDD<Coordinate> getWorkerHull(JavaRDD<String> points){
		JavaRDD<Coordinate> workerConvexHull = points.mapPartitions(new FlatMapFunction<Iterator<String>, Coordinate>() {

			private static final long serialVersionUID = 1L;

			public Iterable<Coordinate> call(Iterator<String> line) throws Exception {
				List<Coordinate> workerConvexHull = new ArrayList<Coordinate>();
				try{
					while(line.hasNext()){
						String s =line.next();
						String cood[] = s.split(",");
						Coordinate c = new Coordinate(Double.parseDouble(cood[0]), Double.parseDouble(cood[1]));
						workerConvexHull.add(c);
					}
				}catch(NumberFormatException e){
					
				}
				ConvexHull convexhull = new ConvexHull(workerConvexHull.toArray(new Coordinate[workerConvexHull.size()]), new GeometryFactory());
				Coordinate []arrayConvexHull = convexhull.getConvexHull().getCoordinates();
				return Arrays.asList(arrayConvexHull);
			}
		});
		return workerConvexHull;
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
