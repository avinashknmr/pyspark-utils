from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import MultilayerPerceptronClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

def vectorize(data, features):
    vectorAssembler = VectorAssembler(inputCols=features, outputCol='features')
    return vectorAssembler.transform(data)

def label_encoder(data, feature):
    stringIndexer = StringIndexer(inputCol=feature, outputCol='label')
    return stringIndexer.fit(data).transform(data)

def train_test_split(data, test_size=0.3, random_state=0):
    train_size = 1 - test_size
    train_data, test_data = data.randomSplit([train_size, test_size], random_state)
    return train_data, test_data

def nn(data, layers, random_state=0, *args, **kwargs):
    neural_net = MultilayerPerceptronClassifier(layers=layers, seed=random_state, *args, **kwargs)
    model = neural_net.fit(data)
    return model

def logistic_regression(data, *args, **kwargs):
    lr = LogisticRegression(*args, **kwargs)
    model = lr.fit(data)
    return model

def evaluate(data, type='binary', metric='accuracy'):
    if type=='binary':
        evaluator = BinaryClassificationEvaluator(labelCol='label', rawPredictionCol='prediction', metricName=metric)
    elif type=='multiclass':
        evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName=metric)
    return evaluator.evaluate(data)

