# Scala implementation
## Example: Command to run the Scala version with 4 files and 4 nodes

spark-submit --master spark://192.168.2.225:7077 /home/ubuntu/Scala/4_files/target/scala-2.11/file_1_2.11-1.3.12.jar --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.executorIdleTimeout=30s >> output4_4file.txt
