"""Data readers and preprocessors."""
__all__ = [
    'SparkTransactionReader', 
    'TransactionPreprocessor',
    'DistributedPreprocessor',
    'ProductionPipeline'
]

def __getattr__(name):
    if name == 'SparkTransactionReader':
        from .spark_reader import SparkTransactionReader
        return SparkTransactionReader
    elif name == 'TransactionPreprocessor':
        from .preprocessor import TransactionPreprocessor
        return TransactionPreprocessor
    elif name == 'DistributedPreprocessor':
        from .preprocessor_distributed import DistributedPreprocessor
        return DistributedPreprocessor
    elif name == 'ProductionPipeline':
        from .preprocessor_distributed import ProductionPipeline
        return ProductionPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

