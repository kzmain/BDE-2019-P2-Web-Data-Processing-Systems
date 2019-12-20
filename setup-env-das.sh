set -e
set -x

module load prun/default
module load python/3.6.0
module load hadoop/2.7.6
module load java/jdk-1.8.0

"$PYTHON" -m venv venv

source venv/bin/activate
export HADOOP_CONF_DIR=/cm/shared/package/hadoop/hadoop-2.7.6/etc/hadoop
export PYTHON=/cm/shared/package/python/3.6.0/bin/python3
export SPARK_HOME=/home/bbkruit/spark-2.4.0-bin-hadoop2.7
export PYSPARK_PYTHON="venv/bin/python"

sh setup-env-local.sh
deactivate