package edu.asu.cse512;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/**
 * Hello world!
 *
 */
public class RangeQuery 
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
    	SparkConf conf=new SparkConf().setAppName("SpatialRange");
		JavaSparkContext sc =new JavaSparkContext(conf);
		//reads each line from the csv file and stores it in RDD
		JavaRDD<String> pointStrings = sc.textFile(args[0]);
		JavaRDD<Vector> pointsSet = pointStrings.map(new Function<String,Vector>()
	    {
            public Vector<Double> call(String line)
            {
                String[] coordinates = line.split(",");
                Vector<Double> vect= new Vector<Double>(coordinates.length); 
                
                for(String coordinate: coordinates)
                {
                	vect.add(Double.parseDouble(coordinate));
                	
                }

                return vect;
            }
        });
		
		//getting the second input
		JavaRDD<String> searchPolyString = sc.textFile(args[1]);
		JavaRDD<Vector> searchPolySet = searchPolyString.map(new Function<String,Vector>()
	    {
            public Vector<Double> call(String line)
            {
                String[] coordinates = line.split(",");
    
                Vector<Double> vect= new Vector<Double>(coordinates.length); 
                
                for(String coordinate: coordinates)
                {
                	vect.add(Double.parseDouble(coordinate));
                	
                }

                return vect;
            }
        });
		
		List<Vector> queryWindow = searchPolySet.collect();
		
		//broadcasting the query window points to all the nodes
		final Broadcast<List<Vector>> broadcast = sc.broadcast(queryWindow);
		
		JavaRDD<String> queryResult = pointsSet.map(new Function<Vector,String>()
	            {
	                public String call(Vector inputVect)
	                {
	                            
	                    List<Vector> querWindow = broadcast.value();
	                    Vector<Double> searchVect = querWindow.get(0);
	                     
	                    if(containsPolygon(searchVect,inputVect))
	                    {	
	                    	double pkey= Double.parseDouble (inputVect.elementAt(0).toString());
							int pointKey = (int) pkey;
	                        return String.valueOf(pointKey);
	                    }
	                    else
	                    {
	                        return "NULL";
	                    }
	                }
	            }
	        );
		JavaRDD<String> output = queryResult.filter(new Function<String,Boolean>()
	            {
	                public Boolean call(String result)
	                {
	                	if(!result.contains("NULL"))
	                		return true;
	                	else
	                		return false;
	                }
	            });
		
		deleteIfExist(args[2]);
		output.coalesce(1).saveAsTextFile(args[2]);


	}
    public static boolean containsPolygon(Vector<Double> searchPolygon,Vector<Double> inputPolygon)
	{	
		double iX1=inputPolygon.elementAt(1);
		double iY1=inputPolygon.elementAt(2);
	
		double sX1=Math.min(searchPolygon.elementAt(0),searchPolygon.elementAt(2));
		double sX2=Math.max(searchPolygon.elementAt(0),searchPolygon.elementAt(2));
		double sY1=Math.min(searchPolygon.elementAt(1),searchPolygon.elementAt(3));
		double sY2=Math.max(searchPolygon.elementAt(1),searchPolygon.elementAt(3));
		
		if((sX1<=iX1)&&(sX2>=iX1)&&(sY1<=iY1)&&(sY2>=iY1))
		{	
			return true;
		}

		
		return false;
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

