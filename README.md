# cse512-15fall-project
Project for CSD512 Distribute and Parallel Database, 15 Fall semester, Arizona State University.

For student, please don't directly for this project.

First check if you have right to create a private repository, if you don't apply for [github edu pack](https://education.github.com/pack).

Create a new repository, and choose import from existing repository. Now you're ready to start.

I strongly every one use different account to commit to your group project. So that you have a commit history for each group members.

<b>ClosestPair</b>
<pre>
/home/hduser/Downloads/spark/bin/spark-submit --class edu.asu.cse512.ClosestPair --jars /home/hduser/Desktop/group15/jts-1.13.jar --master spark://192.168.1.100:7077  /home/hduser/Desktop/jarsss/closestPair-0.1.jar "hdfs://master:54310/content/closestpair.csv" "hdfs://master:54310/content/closestpairoutput"
</pre>

<b>ConvexHull</b>
<pre>
/home/hduser/Downloads/spark/bin/spark-submit --class edu.asu.cse512.convexHull --jars /home/hduser/Desktop/group15/jts-1.13.jar --master spark://192.168.1.100:7077  /home/hduser/Desktop/jarsss/convexHull-0.1.jar "hdfs://master:54310/content/convexhull.csv" "hdfs://master:54310/content/convexhulloutput"
</pre>

<b>FarthestPair</b>
<pre>
/home/hduser/Downloads/spark/bin/spark-submit --class edu.asu.cse512.FarthestPair --jars /home/hduser/Desktop/group15/jts-1.13.jar --master spark://192.168.1.100:7077  /home/hduser/Desktop/jarsss/farthestPair-0.1.jar "hdfs://master:54310/content/farthestpair.csv" "hdfs://master:54310/content/farthestpairoutput"
</pre>

<b>JoinQuery</b>
<pre>
/home/hduser/Downloads/spark/bin/spark-submit --class edu.asu.cse512.Join --jars /home/hduser/Desktop/group15/jts-1.13.jar --master spark://192.168.1.100:7077  /home/hduser/Desktop/jarsss/joinQuery-0.1.jar "hdfs://master:54310/content/joinquery1.csv" "hdfs://master:54310/content/joinquery2.csv" "hdfs://master:54310/content/joinqueryoutput" "rectangle"
</pre>

<pre>
/home/hduser/Downloads/spark/bin/spark-submit --class edu.asu.cse512.Join --jars /home/hduser/Desktop/group15/jts-1.13.jar --master spark://192.168.1.100:7077  /home/hduser/Desktop/jarsss/joinQuery-0.1.jar "hdfs://master:54310/content/joinquery3.csv" "hdfs://master:54310/content/joinquery2.csv" "hdfs://master:54310/content/joinqueryoutput" "point"
</pre>

<b>RangeQuery</b>
<pre>
/home/hduser/Downloads/spark/bin/spark-submit --class edu.asu.cse512.RangeQuery --jars /home/hduser/Desktop/group15/jts-1.13.jar --master spark://192.168.1.100:7077  /home/hduser/Desktop/jarsss/rangeQuery-0.1.jar "hdfs://master:54310/content/rangequery1.csv" "hdfs://master:54310/content/rangequery2.csv" "hdfs://master:54310/content/rangequeryoutput"
</pre>

<b>GeometricUnion</b>
<pre>
/home/hduser/Downloads/spark/bin/spark-submit --class edu.asu.cse512.Union --jars /home/hduser/Desktop/group15/jts-1.13.jar --master spark://192.168.1.100:7077  /home/hduser/Desktop/jarsss/union-0.1.jar "hdfs://master:54310/content/union.csv" "hdfs://master:54310/content/unionoutput" 
</pre>


