from .processing import StoreNormalizer

__all__ = ['SPAN_STATUS_MAPPING']


def _get_span_status_mapping():
    normalizer = StoreNormalizer()
    rv = {}

    for i in range(255):
        name = normalizer.normalize_event({
            'contexts': {
                'trace': {
                    'status': i,
                },
            },
        })['contexts']['trace']['status']
        if name is not None:
            rv[i] = name

    return rv


SPAN_STATUS_MAPPING = _get_span_status_mapping()
del _get_span_status_mapping