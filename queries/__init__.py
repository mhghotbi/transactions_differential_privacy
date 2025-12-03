"""Query definitions for transaction metrics."""
__all__ = [
    'TransactionCountQuery',
    'UniqueCardsQuery', 
    'UniqueAcceptorsQuery',
    'TotalAmountQuery',
    'TransactionWorkload'
]

def __getattr__(name):
    if name in __all__:
        from .transaction_queries import (
            TransactionCountQuery,
            UniqueCardsQuery,
            UniqueAcceptorsQuery,
            TotalAmountQuery,
            TransactionWorkload
        )
        return {
            'TransactionCountQuery': TransactionCountQuery,
            'UniqueCardsQuery': UniqueCardsQuery,
            'UniqueAcceptorsQuery': UniqueAcceptorsQuery,
            'TotalAmountQuery': TotalAmountQuery,
            'TransactionWorkload': TransactionWorkload
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

