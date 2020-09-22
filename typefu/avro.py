from datetime import datetime
mapper = {
        "string": str,
        "long": int,
        "int": int,
        "boolean": [int, bool],
        "double": float,
        "timestamp-millis": datetime.strptime
    }