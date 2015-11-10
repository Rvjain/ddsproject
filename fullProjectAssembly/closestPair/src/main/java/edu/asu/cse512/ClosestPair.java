package edu.asu.cse512;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

public class ClosestPair {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("DDSProject");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> closestPair = getClosestPair(context, args[0]);
		deleteIfExist(args[1]);
		closestPair.saveAsTextFile(args[1]);
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

	private static JavaRDD<String> getClosestPair(JavaSparkContext context, String path) {

		JavaRDD<String> points = context.textFile(path);
		JavaRDD<Coordinate> closestPair = points.mapPartitions(new WorkerMapFunction());
		JavaRDD<Coordinate> temp = closestPair.coalesce(1);
		JavaRDD<String> finalClosestPair = temp.mapPartitions(new FlatMapFunction<Iterator<Coordinate>, String>() {
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(Iterator<Coordinate> coordinate) throws Exception {
				List<Coordinate> lisOfPoint = new ArrayList<Coordinate>();
				while (coordinate.hasNext()) {
					Coordinate c = coordinate.next();
					lisOfPoint.add(c);
				}
				PairOfPoint pair = ClosestPairUtilFunctions.divideAndConquer(lisOfPoint);
				ArrayList<String> listOfClosestCoordinate = new ArrayList<String>();
				listOfClosestCoordinate.add(pair.getP1().x + ", " + pair.getP1().y);
				listOfClosestCoordinate.add(pair.getP2().x + ", " + pair.getP2().y);
				return listOfClosestCoordinate;
			}
		});
		return finalClosestPair;
	}

}

class WorkerMapFunction implements FlatMapFunction<Iterator<String>, Coordinate>, Serializable {
	private static final long serialVersionUID = 1L;

	public Iterable<Coordinate> call(Iterator<String> line) throws Exception {
		List<Coordinate> lisOfPoint = new ArrayList<Coordinate>();
		try {
			while (line.hasNext()) {

				String l = line.next();
				String points[] = l.split(",");
				if (isNumeric(points[0]) && isNumeric(points[1])) {
					lisOfPoint.add(new Coordinate(Double.parseDouble(points[0]), Double.parseDouble(points[1])));
				}

			}
		} catch (NumberFormatException e) {

		}
		PairOfPoint pair = ClosestPairUtilFunctions.divideAndConquer(lisOfPoint);
		ArrayList<Coordinate> listOfClosestCoordinate = new ArrayList<Coordinate>();
		listOfClosestCoordinate.add(pair.getP1());
		listOfClosestCoordinate.add(pair.getP2());

		return listOfClosestCoordinate;
	}

	public static boolean isNumeric(String str) {
		try {
			double d = Double.parseDouble(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}
}

class ClosestPairUtilFunctions {

	public static PairOfPoint divideAndConquer(List<Coordinate> lisOfPoint) {
		ArrayList<Coordinate> xList = new ArrayList<Coordinate>(lisOfPoint);
		sortByXCoordinate(xList);
		ArrayList<Coordinate> yList = new ArrayList<Coordinate>(lisOfPoint);
		sortByYCoordinate(yList);
		return divideAndConquer(xList, yList);
	}

	private static PairOfPoint getClosestPair(List<Coordinate> xSorted) {
		int size = xSorted.size();
		if (size < 2)
			return null;
		PairOfPoint closestPair = new PairOfPoint(xSorted.get(0), xSorted.get(1));
		if (size > 2) {
			for (int i = 0; i < size - 1; i++) {
				for (int j = i + 1; j < size; j++) {
					double dist = xSorted.get(i).distance(xSorted.get(j));
					if (dist < closestPair.getDistance()) {
						closestPair.setP1(xSorted.get(i));
						closestPair.setP2(xSorted.get(j));
						closestPair.setDistance(dist);
					}
				}
			}
		}
		return closestPair;
	}

	public static PairOfPoint divideAndConquer(List<Coordinate> xSorted, List<Coordinate> ySorted) {
		int xSortedSize = xSorted.size();
		if (xSorted.size() <= 3) {
			return getClosestPair(xSorted);
		}
		int midpoint = xSortedSize >>> 1;
		List<Coordinate> leftPart = xSorted.subList(0, midpoint);
		List<Coordinate> rightPart = xSorted.subList(midpoint, xSortedSize);
		ArrayList<Coordinate> yLeftPart = new ArrayList<Coordinate>(leftPart);
		sortByYCoordinate(yLeftPart);
		PairOfPoint leftClosestPair = divideAndConquer(leftPart, yLeftPart);

		ArrayList<Coordinate> yRighttPart = new ArrayList<Coordinate>(rightPart);
		sortByYCoordinate(yRighttPart);
		PairOfPoint rightClosestPair = divideAndConquer(rightPart, yRighttPart);

		PairOfPoint closestPair = new PairOfPoint();

		if (leftClosestPair.getDistance() < rightClosestPair.getDistance()) {
			closestPair = leftClosestPair;
		} else {
			closestPair = rightClosestPair;
		}

		double closestDistance = closestPair.getDistance();
		double centerPointX = rightPart.get(0).x;
		ArrayList<Coordinate> strippedCoordinateList = new ArrayList<Coordinate>();
		for (Coordinate coordinate : ySorted) {
			if (Math.abs(centerPointX - coordinate.x) < closestDistance) {
				strippedCoordinateList.add(coordinate);
			}
		}

		for (int i = 0; i < strippedCoordinateList.size() - 1; i++) {
			for (int j = i + 1; j < strippedCoordinateList.size(); j++) {
				if ((strippedCoordinateList.get(j).y - strippedCoordinateList.get(i).y) >= closestDistance) {
					break;
				}
				double dist = strippedCoordinateList.get(i).distance(strippedCoordinateList.get(j));
				if (dist < closestPair.getDistance()) {
					closestPair.setP1(strippedCoordinateList.get(i));
					closestPair.setP2(strippedCoordinateList.get(j));
					closestPair.setDistance(dist);
					closestDistance = dist;
				}

			}
		}

		return closestPair;

	}

	private static void sortByXCoordinate(List<Coordinate> lisOfPoint) {
		Collections.sort(lisOfPoint, new Comparator<Coordinate>() {
			public int compare(Coordinate o1, Coordinate o2) {
				if (o1.x < o2.x)
					return -1;
				if (o1.x > o2.x)
					return 1;
				return 0;
			}
		});
	}

	private static void sortByYCoordinate(List<Coordinate> lisOfPoint) {
		Collections.sort(lisOfPoint, new Comparator<Coordinate>() {
			public int compare(Coordinate o1, Coordinate o2) {
				if (o1.y < o2.y)
					return -1;
				if (o1.y > o2.y)
					return 1;
				return 0;
			}
		});

	}

}

class PairOfPoint implements Serializable {
	private static final long serialVersionUID = 1L;
	private Coordinate p1 = null;
	private Coordinate p2 = null;
	private double distance = 0.0;

	public PairOfPoint(Coordinate p1, Coordinate p2) {
		this.p1 = p1;
		this.p2 = p2;
		this.distance = calculateDistance(p1, p2);
	}

	public PairOfPoint() {
		// TODO Auto-generated constructor stub
	}

	private double calculateDistance(Coordinate p1, Coordinate p2) {
		return p1.distance(p2);
	}

	public Coordinate getP1() {
		return p1;
	}

	public void setP1(Coordinate p1) {
		this.p1 = p1;
	}

	public Coordinate getP2() {
		return p2;
	}

	public void setP2(Coordinate p2) {
		this.p2 = p2;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

}
