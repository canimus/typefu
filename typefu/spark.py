import pyspark.sql.types as T
from operator import itemgetter as ig

mapper = {
        "long": T.LongType(),
        "string": T.StringType(),
        "int": T.IntegerType(),
        "boolean": T.BooleanType(),
        "double": T.DoubleType(),
        "float": T.FloatType(),
        "timestamp-millis": T.TimestampType()
    }

def map_field(name, data_type, nullable):  
    if isinstance(data_type, str):
        return T.StructField(name, mapper[data_type], bool(nullable))
    try:
        data_type = ig(1)(data_type)
        return map_field(name, data_type, bool(nullable))
    except:
        return map_field(name, ig('logicalType')(data_type), bool(nullable))

def build_schema(json_schema):        
    schema = T.StructType()
    _f = lambda x: map_field(*ig(*('name', 'type', 'nullable'))(x))
    [schema.add(x) for x in map(lambda x: _f(x), json_schema['fields'])]
    return schema