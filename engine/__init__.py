"""DP engines for noise injection."""
__all__ = ['TopDownEngine']

def __getattr__(name):
    if name == 'TopDownEngine':
        from .topdown import TopDownEngine
        return TopDownEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

