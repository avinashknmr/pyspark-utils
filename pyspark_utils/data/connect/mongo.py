def read(spark, connection, database, collection, *args, **kwargs):
    options = {
        "connection.uri": connection,
        "database": database,
        "collection": collection
    }
    options = options | kwargs
    dataframe = spark.read.format('mongodb').options(**options).load()
    return dataframe

def write(dataframe, connection, database, collection, mode='append', *args, **kwargs):
    try:
        options = {
            "connection.uri": connection,
            "database": database,
            "collection": collection
        }
        options = options | kwargs
        dataframe.write.format('mongodb').mode(mode).options(**options).save()
        return True
    except:
        return False

