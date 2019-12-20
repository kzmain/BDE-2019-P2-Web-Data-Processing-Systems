set -e
set -x

export HADOOP_CONF_DIR=/cm/shared/package/hadoop/hadoop-2.7.6/etc/hadoop
export PYTHON=/cm/shared/package/python/3.6.0/bin/python3
export SPARK_HOME=/home/bbkruit/spark-2.4.0-bin-hadoop2.7
export PYSPARK_PYTHON="venv/bin/python"

sh setup-env-das.sh

pushd venv/
zip -rq ../venv.zip *
popd

zip -rq src.zip Extractor/ Linking/ NLP/ System/ Tools/


spark-submit \
    --name "wdps01-pipeline" \
    --master yarn \
    --deploy-mode cluster \
    --conf "spark.yarn.appMasterEnv.SPARK_HOME=$SPARK_HOME" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON" \
    --conf "spark.yarn.appMasterEnv.SAMPLE_SIZE=10" \
    --archives "venv.zip#venv" \
    --py-files "src.zip" \
    A1.py
