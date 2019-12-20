set -e
set -x

module load prun/default
module load python/3.6.0
module load hadoop/2.7.6
module load java/jdk-1.8.0

export HADOOP_CONF_DIR=/cm/shared/package/hadoop/hadoop-2.7.6/etc/hadoop
export PYTHON=/cm/shared/package/python/3.6.0/bin/python3
export SPARK_HOME=/home/bbkruit/spark-2.4.0-bin-hadoop2.7/

"$PYTHON" -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.pip
python3 -m spacy download en_core_web_sm
deactivate

pushd venv/
zip -rq ../venv.zip *
popd

zip -rq src.zip Extractor/ Linking/ NLP/ System/ Tools/

export PYSPARK_PYTHON="venv/bin/python"

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
