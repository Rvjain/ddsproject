package edu.asu.cse512;

import java.io.IOException;
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

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import org.apache.spark.api.java.function.Function;

/**
 * Hello world!
 *
 */
public class convexHull 
{
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	*/
    public static void main( String[] args )
    {
        //Initialize, need to remove existing in output file location.
    	
    	//Implement 
    	
    	//Output your result, you need to sort your result!!!
    	//And,Don't add a additional clean up step delete the new generated file...
    	SparkConf sparkConf = new SparkConf().setAppName("DDSProject");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<Coordinate> masterConvexHull = getConvexHull(context, args[0]);
		deleteIfExist(args[1]);
		masterConvexHull.distinct().sortBy( new Function<Coordinate,Coordinate>() {

			public Coordinate call(Coordinate str) throws Exception {
				// TODO Auto-generated method stub
				return str;
			}
			}, true, 1 ).saveAsTextFile(args[1]);

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
    public static JavaRDD<Coordinate> getConvexHull(JavaSparkContext context, String filePath) {
		JavaRDD<String> points = context.textFile(filePath);
		JavaRDD<Coordinate> workerConvexHull = getWorkerHull(points);
		workerConvexHull = workerConvexHull.coalesce(1);
		JavaRDD<Coordinate> masterConvexHull = getMasterHull(workerConvexHull);
		return masterConvexHull;
	}
	
	public static JavaRDD<Coordinate> getMasterHull(JavaRDD<Coordinate> workerConvexHull) {
		JavaRDD<Coordinate> masterHull = workerConvexHull.mapPartitions(new FlatMapFunction<Iterator<Coordinate>, Coordinate>() {

			private static final long serialVersionUID = 1L;

			public Iterable<Coordinate> call(Iterator<Coordinate> coordinate) throws Exception {
				List<Coordinate> masterConvexHull = new ArrayList<Coordinate>();
				while(coordinate.hasNext()){
					Coordinate c =coordinate.next();
					masterConvexHull.add(c);
				}
				ConvexHull convexhull = new ConvexHull(masterConvexHull.toArray(new Coordinate[masterConvexHull.size()]), new GeometryFactory());
				Coordinate []arrayConvexHull = convexhull.getConvexHull().getCoordinates();
				//ArrayList<String> result = new ArrayList<String>();
//				for(int i=0;i<arrayConvexHull.length;i++){
//					String s = arrayConvexHull[i].x+", "+arrayConvexHull[i].y;
//					result.add(s);
//				}
				return Arrays.asList(arrayConvexHull);
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

}
