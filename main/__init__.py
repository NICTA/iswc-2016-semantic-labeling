import os

from gensim.models import Word2Vec

# configure spark to be started with more allocated memory
memory = '12g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

from pyspark import SparkConf, SparkContext, SQLContext

conf = (SparkConf()
        .setMaster("local")
        .setAppName("KarmaDSL")
        .set("spark.executor.cores","8")
        .set("spark.executor.memory", "1g")
        )

sc = SparkContext(conf=conf)
sql_context = SQLContext(sc)

root_dir = os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
data_dir = os.path.join(root_dir, "data/datasets")
train_model_dir = os.path.join(root_dir, "data/train_models")

# word2vec = Word2Vec.load_word2vec_format(os.path.join("/Users/minhpham/tools/", 'GoogleNews-vectors-negative300.bin'), binary=True)

file_write = open('debug.txt', 'w')

logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org").setLevel(logger.Level.FATAL)
logger.LogManager.getLogger("akka").setLevel(logger.Level.FATAL)
