This is a simple HDFS Monitoring project using Spark Streaming and Scala.

There are 3 tasks:
* Task A:  For each RDD of Dstream, the word frequency of a text file is calculated and saved on HDFS.
* Task B: The same as A except in this case, the co-occurrence frequency is calculated. Here, only words that appear in the same line are considered as "co-occurred".
* Task C is identical to B except here the updates are stateful (using updateStateByKey).

How to run?
1 - Copy the scala-count1.jar file into AWS EMR cluster
2 - Run the monitoring program using the following command:
`spark-submit --class org.example.SparkWordCount --master yarn --deploy-mode client scala-count1.jar hdfs:///input hdfs:///output`

3 - After initiating the execution, copy input files into the input directory one after another. Kindly provide a buffer period 15-20 seconds before moving an input file into the directory.
4 - Once the files are moved, kindly terminate the program after 15-20 seconds (this is to stop the task C code from continuously creating new output directories)

Expected Outputs:

Input text: 'I*** like pig latin I like hive too2 I don’t like hive too.'

Task A:
![image](https://github.com/as2811-project/Simple-Scala-Word-Count/assets/83534298/bff7175e-8be0-41f1-b360-cbf711bb59b4)

Task B:
![image](https://github.com/as2811-project/Simple-Scala-Word-Count/assets/83534298/88d71290-9534-4ef4-9329-a2d94037a87c)

Task C (input text was simply copied again):
![image](https://github.com/as2811-project/Simple-Scala-Word-Count/assets/83534298/1a4ecf42-fdff-45bf-a589-2e6c538e0a7e)
