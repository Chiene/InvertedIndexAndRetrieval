# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/CalculateAverage/Output/
#hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage /user/shared/CalculateAverage/Input /user/TA/CalculateAverage/Output
#hdfs dfs -cat /user/TA/CalculateAverage/Output/part-*
invertedIndexOutput=InvertedIndexOutput_backup
input_directory=InvertedIndexInput_backup
your_hadoop_output_directory=RetrievalOutput
hdfs dfs -rm -r $your_hadoop_output_directory
hadoop jar Retrieval.jar Retrieval.Retrieval $invertedIndexOutput $your_hadoop_output_directory $input_directory $1
hdfs dfs -cat $your_hadoop_output_directory/part-*
