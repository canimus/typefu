import pyspark.sql.types as T
from operator import itemgetter as ig
import requests
import json

mapper = {
        "long": T.LongType(),
        "string": T.StringType(),
        "int": T.IntegerType(),
        "boolean": T.BooleanType(),
        "double": T.DoubleType(),
        "float": T.FloatType(),
        "timestamp-millis": T.TimestampType()
    }

def get_registry(url, entity):
    return json.loads(
            requests\
            .get(url.format(entity))\
            .json().get("schemaText")
        )

def get_field(name, data_type, nullable):  
    if isinstance(data_type, str):
        return T.StructField(name, mapper[data_type], bool(nullable))
    try:
        data_type = ig(1)(data_type)
        return get_field(name, data_type, bool(nullable))
    except:
        return get_field(name, ig('logicalType')(data_type), bool(nullable))

def get_schema(json_schema):
    schema = T.StructType()
    _f = lambda x: get_field(*ig(*('name', 'type', 'nullable'))(x))
    [schema.add(x) for x in map(lambda x: _f(x), json_schema['fields'])]
    return schema