my_dir=`dirname $0`
source $my_dir/var.sh
declare hdfs_input_dir="$1"
declare hdfs_output_dir="$2"
declare runs_per_folder=4

for((i=0; i<size_length; i++))
do
  for((j=0; j<count_length; j++))
  do
    for((k=0; k<$runs_per_folder; k++))
    do
      spark-submit --class FileDetectorHdfs --master yarn --deploy-mode cluster --driver-memory 6g --executor-memory 16g --num-executors 5 --executor-cores 10 --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --queue default jars/tmp/spark-tika.jar ${hdfs_input_dir}/orc_${sizes[i]}mb_${intervals[j]}_files/ ${hdfs_output_dir} 
      sleep 5
    done
  done
done
