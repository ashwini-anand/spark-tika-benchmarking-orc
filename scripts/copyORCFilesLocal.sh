my_dir=`dirname $0`
source $my_dir/var.sh
declare local_input_dir="$1"
declare hdfs_target_dir="$2"

(echo
echo "———————————————————————————————————————New Log Starts————————————————"
date
echo

for((i=0; i<size_length; i++))
do
  for((j=0; j<count_length; j++))
  do
    su - hdfs -c "hdfs dfs -mkdir -p ${hdfs_target_dir}/orc_${sizes[i]}mb_${intervals[j]}_files"
    su - hdfs -c "hdfs dfs -chown $USER ${hdfs_target_dir}/orc_${sizes[i]}mb_${intervals[j]}_files"
    time(for((k=0; k<${counts[$j]}; k++))
    do
      hdfs dfs -copyFromLocal ${local_input_dir}/orc_${sizes[i]}.orc ${hdfs_target_dir}/orc_${sizes[i]}mb_${intervals[j]}_files/orc_${sizes[i]}mb_${intervals[j]}_$k.orc
    done
    echo ""
    echo transferred ${counts[j]} ${sizes[i]}mb files in below given time:)
  done
done) >> logs.txt 2>&1
