import pyspark.sql.types as T
import pyspark.sql.functions as F
from operator import itemgetter as ig
import requests
import json
from alphareader import AlphaReader
import pyarrow as pa

mapper = {
    "long": T.LongType(),
    "string": T.StringType(),
    "int": T.IntegerType(),
    "boolean": T.BooleanType(),
    "double": T.DoubleType(),
    "float": T.FloatType(),
    "timestamp-millis": T.TimestampType()
}

def get_registry(url, entity='user_en', version='latest'):
    '''http://server:port/api/v1/schemaregistry/schemas/{entity}/versions/{version}'''
    return json.loads(
        requests
        .get(url.format(entity, version))
        .json().get("schemaText")
    )

def get_field(name, data_type, nullable):
    if isinstance(data_type, str):
        return T.StructField(name, ig(data_type)(mapper), bool(nullable))
    try:
        return get_field(name, ig(1)(data_type), bool(nullable))
    except:
        return get_field(name, ig('logicalType')(data_type), bool(nullable))

def get_schema(json_schema):
    schema = T.StructType()
    def _f(x): return get_field(*ig(*('name', 'type', 'nullable'))(x))
    [schema.add(x) for x in map(lambda x: _f(x), json_schema['fields'])]
    return schema

def get_types(schema):
    return list(map(lambda x: (x.name, x.dataType), schema.fields))
    
def convert_dat_to_parquet(spark, path, url, entity='user_en', version='latest'):
    fs = pa.hdfs.connect()
    schema = get_schema(get_registry(url, entity, version))
    df = spark.createDataFrame(AlphaReader(fs.open(path, 'rb')))
    data_types = get_types(schema)
    _new = map(lambda x: x[0], data_types)
    _old = df.columns
    
    return df\
        .select([F.col(o).alias(n) for o, n in zip(_old, _new)])\
        .select([F.col(c).cast(t) for c, t in data_types])