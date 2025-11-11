from twine import wheel

from packaging import metadata

import sys

filepaths = sys.argv[1:]

for fp in filepaths:
    dist = wheel.Wheel(fp)
    data = dist.read()
    meta, unparsed = metadata.parse_email(data)

    if unparsed:
        raise SystemExit(
            f"unexpected unparsed metadata while processing {fp}:\n{unparsed}"
        )

    try:
        metadata.Metadata.from_raw(meta)
    except metadata.ExceptionGroup as group:
        raise SystemExit(
            "Invalid distribution metadata ({}): {}".format(
                fp, "; ".join(sorted(str(e) for e in group.exceptions))
            )
        )
