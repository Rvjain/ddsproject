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

public class convexHull 
{
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	*/
    
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

}
