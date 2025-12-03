"""Schema definitions for geography and histograms."""
__all__ = ['Geography', 'Province', 'City', 'TransactionHistogram']

def __getattr__(name):
    if name in ('Geography', 'Province', 'City'):
        from .geography import Geography, Province, City
        return {'Geography': Geography, 'Province': Province, 'City': City}[name]
    elif name == 'TransactionHistogram':
        from .histogram import TransactionHistogram
        return TransactionHistogram
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

