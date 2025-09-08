from importlib.metadata import version

from .httpfs import HttpFs

__version__ = version("simple-httpfs")

__all__ = ["HttpFs", "__version__"]

del version
