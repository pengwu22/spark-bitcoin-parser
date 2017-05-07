#!/bin/sh
files="../../blk00088.dat ../../blk00089.dat"


echo "###### Spark Parsing Workflow Starts... ######"
spark-submit \
  --master local[4] \
  spark_parser.py $files &&
# Better Data Storage:
cat csv/inputs_mapping/part* | sed -e '/^ *$/d' > csv/inputs_mapping.csv &&
cat csv/outputs/part* | sed -e '/^ *$/d' > csv/outputs.csv &&
cat csv/transactions/part* | sed -e '/^ *$/d' > csv/transactions.csv &&
rm -r csv/inputs_mapping &&
rm -r csv/outputs &&
rm -r csv/transactions &&
#
echo "\n###### Step 1 Done ######\n" &&
spark-submit spark_mapinput.py &&
echo "\n###### Step 2 Done ######\n" &&
spark-submit spark_mapaddr.py &&
echo "\n###### Step 3 Done ######\n" &&
echo "\n###### Parsed Files: ######\n"$files
