# avro_helper.py
import io
from fastavro import parse_schema, schemaless_writer, schemaless_reader
import json

def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    return parse_schema(schema)

def serialize_avro(schema, record):
    """
    Returns bytes of Avro-serialized record (schemaless).
    """
    bytes_io = io.BytesIO()
    schemaless_writer(bytes_io, schema, record)
    return bytes_io.getvalue()

def deserialize_avro(schema, data_bytes):
    """
    Read a single schemaless Avro record from bytes.
    """
    bytes_io = io.BytesIO(data_bytes)
    record = schemaless_reader(bytes_io, schema)
    return record
