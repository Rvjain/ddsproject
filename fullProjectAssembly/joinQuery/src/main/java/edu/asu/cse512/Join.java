package edu.asu.cse512;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;



import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class Join 
{
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation 1
	 * @param inputLocation 2
	 * @param outputLocation
	 * @param inputType 
	 * 
	*/
    public static void main( String[] args )
    {
        //Initialize, need to remove existing in output file location.
    	
    	//Implement 
    	
    	//Output your result, you need to sort your result!!!
    	//And,Don't add a additional clean up step delete the new generated file...
    	SparkConf conf=new SparkConf().setAppName("SpatialJoin");
		JavaSparkContext sc =new JavaSparkContext(conf);
		//reads each line from the csv file and stores it in RDD
		
		JavaRDD<String> inputSet = sc.textFile(args[0]);
		JavaRDD<Vector> pointsSet = inputSet.map(new Function<String,Vector>()
				{
		            public Vector<Double> call(String line)
		            {
		                String[] coordinates = line.split(",");
		                Vector<Double> vect= new Vector<Double>(coordinates.length);
		                for(String coordinate: coordinates)
		                	vect.add(Double.parseDouble(coordinate));
		                
		                return vect;	
		            }       
		        });
		//getting the second input
		
		JavaRDD<String> searchPolyStrings = sc.textFile(args[1]);
		JavaRDD<Vector> searchPolySet = searchPolyStrings.map(new Function<String,Vector>()
			    {
		            public Vector<Double> call(String line)
		            {
		                String[] coordinates = line.split(",");
		                Vector<Double> vect= new Vector<Double>(coordinates.length); 
		       
		                for(String coordinate: coordinates)
		                	vect.add(Double.parseDouble(coordinate));
		                	
		                return vect;
		            }
		        });
		
		List<Vector> queryWindows = searchPolySet.collect();
		
		//broadcasting the query window points to all the nodes
		final Broadcast<List<Vector>> broadcast = sc.broadcast(queryWindows);
		
		JavaRDD<Tuple2<String,String>>  windowInputTuple= pointsSet.flatMap(new FlatMapFunction<Vector,Tuple2<String,String>>()
				{
					public List<Tuple2<String, String>> call(Vector inputVect)
					{
						List<Tuple2<String,String>> tupleList = new ArrayList<Tuple2<String,String>>();
						List<Vector> querWindows = broadcast.value();
						
						//iterate through every query window
						for(Vector window: querWindows)
						{
							if(containsPolygon(window,inputVect))
							{
								double wkey= Double.parseDouble (window.elementAt(0).toString());
								int windowKey = (int) wkey;
								
								double inKey= Double.parseDouble (inputVect.elementAt(0).toString());
								int inputKey = (int) inKey;
								
								tupleList.add(new Tuple2(String.valueOf(windowKey),String.valueOf(inputKey)));
							}
								
						}
						
						return tupleList;
					}
				
				}); 

		 JavaPairRDD<String, Iterable<String>> windowInputPairs = windowInputTuple.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
             public Tuple2<String, String> call(Tuple2<String,String> tup) {
                 return new Tuple2(tup._1().toString(), tup._2().toString());
             }
             }).sortByKey().groupByKey();
		
		 JavaRDD<String> finalOutput= windowInputPairs.map(new Function<Tuple2<String,Iterable<String>>,String>()
				    {
	            		public String call(Tuple2<String,Iterable<String>> tup)
	            		{   StringBuffer sBuffer = new StringBuffer(tup._1());
	            	       
	            			for(String value: tup._2())
	            			{
	            				sBuffer.append(",");
	            				sBuffer.append(value);
	            			}
	                	
	            			return sBuffer.toString();
	            		}
	      });
		
		//reduce the function to get the output string
		 deleteIfExist(args[2]);
		finalOutput.coalesce(1).sortBy( new Function<String,String>() {

			public String call(String str) throws Exception {
				// TODO Auto-generated method stub
				return str;
			}
			}, true, 1 ).saveAsTextFile(args[2]);
		 
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

    
//This function returns whether the given rectangle or point is in the Query Rectangle
	public static boolean containsPolygon(Vector<Double> searchPolygon,Vector<Double> inputVect)
	{
		
		double sXmin=Math.min(searchPolygon.elementAt(1),searchPolygon.elementAt(3));
		double sXmax=Math.max(searchPolygon.elementAt(1),searchPolygon.elementAt(3));
		double sYmin=Math.min(searchPolygon.elementAt(2),searchPolygon.elementAt(4));
		double sYmax=Math.max(searchPolygon.elementAt(2),searchPolygon.elementAt(4));
		
		//if the input vector supplied is point
		if(inputVect.size()==3)
		{	
			double iX=inputVect.elementAt(1);
			double iY=inputVect.elementAt(2);
			if((sXmin<=iX)&&(sXmax>=iX)&&(sYmin<=iY)&&(sYmax>=iY))
			{	
				return true;
			}
		}
		//if input supplied is rectangle
		if(inputVect.size()==5)
		{   
			double iXmin=Math.min(inputVect.elementAt(1),inputVect.elementAt(3));
			double iXmax=Math.max(inputVect.elementAt(1),inputVect.elementAt(3));
			double iYmin=Math.min(inputVect.elementAt(2),inputVect.elementAt(4));
			double iYmax=Math.max(inputVect.elementAt(2),inputVect.elementAt(4));
      	
			if((sXmin<=iXmin)&&(sXmax>=iXmax)&&(sYmin<=iYmin)&&(sYmax>=iYmax))
			{	
				return true;
			}	
		}

		return false;
	}

}
