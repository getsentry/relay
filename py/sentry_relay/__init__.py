from sentry_relay import _relay_pyo3

__all__ = []
__doc__ = _relay_pyo3.__doc__

if hasattr(_relay_pyo3, "__all__"):
    __all__ = _relay_pyo3.__all__


def _import_all():
    submodules = ["auth", "consts", "exceptions", "processing"]
    glob = globals()
    for modname in submodules:
        if modname[:1] == "_":
            continue
        mod = __import__("sentry_relay.%s" % modname, glob, glob, ["__name__"])
        if not hasattr(mod, "__all__"):
            continue
        __all__.extend(mod.__all__)
        for name in mod.__all__:
            obj = getattr(mod, name)
            if hasattr(obj, "__module__"):
                obj.__module__ = "sentry_relay"
            glob[name] = obj


_import_all()
del _import_all
