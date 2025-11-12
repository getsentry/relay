import zipfile

from packaging import metadata

import sys

filepaths = sys.argv[1:]

for fp in filepaths:
    zipf = zipfile.ZipFile(fp)
    names = zipf.namelist()

    data = next((zipf.read(name) for name in names if "METADATA" in name), b"")
    parsed, unparsed = metadata.parse_email(data)

    if unparsed:
        raise SystemExit(
            f"unexpected unparsed metadata while processing {fp}:\n{unparsed}"
        )

    try:
        metadata.Metadata.from_raw(parsed)
    except metadata.ExceptionGroup as group:
        raise SystemExit(
            "Invalid distribution metadata ({}): {}".format(
                fp, "; ".join(sorted(str(e) for e in group.exceptions))
            )
        )
