set -e
set -x

module load prun/default
module load python/3.6.0
module load hadoop/2.7.6
module load java/jdk-1.8.0

export PYTHON=/cm/shared/package/python/3.6.0/bin/python3
export SPARK_HOME=/home/wdps1901/spark-2.4.4-bin-hadoop2.7

# Set $PYTHON to the Python executable you want to create
# your virtual environment with. It could just be something
# like `python3`, if that's already on your $PATH, or it could
# be a /fully/qualified/path/to/python.
test -n "$PYTHON"

# Make sure $SPARK_HOME is on your $PATH so that `spark-submit`
# runs from the correct location.
test -n "$SPARK_HOME"

"$PYTHON" -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.pip
deactivate

# Here we package up an isolated environment that we'll ship to YARN.
# The awkward zip invocation for venv just creates nicer relative
# paths.
pushd venv/
zip -rq ../venv.zip *
popd

# Here it's important that application/ be zipped in this way so that
# Python knows how to load the module inside.
zip -rq src.zip src/

# We want YARN to use the Python from our virtual environment,
# which includes all our dependencies.
export PYSPARK_PYTHON="venv/bin/python"

# YARN Cluster Mode Example
# -------------------------
# Two additional tips when running in cluster mode:
#  1. Be sure not to name your driver script (in this example, hello.py)
#     the same name as your application folder. This confuses Python when it
#     tries to import your module (e.g. `import application`).
#  2. Since your driver is running on the cluster, you'll need to
#     replicate any environment variables you need using
#     `--conf "spark.yarn.appMasterEnv..."` and any local files you
#     depend on using `--files`.
spark-submit \
    --name "wdps01-pipeline" \
    --master yarn \
    --deploy-mode cluster \
    --conf "spark.yarn.appMasterEnv.SPARK_HOME=$SPARK_HOME" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON" \
    --archives "venv.zip#venv" \
    --py-files "src.zip" \
    src/A1.py
