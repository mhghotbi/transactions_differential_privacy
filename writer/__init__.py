"""Output writers."""
__all__ = ['ParquetWriter', 'CSVWriter']

def __getattr__(name):
    if name == 'ParquetWriter':
        from .parquet_writer import ParquetWriter
        return ParquetWriter
    elif name == 'CSVWriter':
        from .parquet_writer import CSVWriter
        return CSVWriter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

