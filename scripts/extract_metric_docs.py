# Extracts documentation for the metrics used in Relay from the code docs

import re
from os import path
from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from jinja2 import Environment, FileSystemLoader


@dataclass
class FileDef:
    name: str
    path: str


@dataclass
class ExtractorDef:
    file: str
    processor: str
    processor_config: Any
    formatter: str
    formatter_config: Any


@dataclass
class CommandDef:
    name: str
    fn: Callable[[str, Any], Any]


@dataclass
class Metric:
    name: str
    id: str
    feature: Optional[str]
    description: List[str]


@dataclass
class MetricType:
    name: str
    human_name: str
    description: List[str]
    metrics: List[Metric]


files = [
    FileDef(name="metrics", path="../relay-server/src/metrics.rs"),
]

extractions = [
    ExtractorDef(
        file="metrics",
        processor="extract_metrics",
        processor_config=[
            "RelayHistograms",
            "RelayTimers",
            "RelaySets",
            "RelayCounters",
        ],
        formatter="metrics_to_markdown",
        formatter_config="../docs/configuration/metrics.md",
    )
]


def _get_file_path(relative_file_name):
    base_path = path.split(__file__)[0]
    return path.join(base_path, relative_file_name)


def extract_metrics(file_name, enums):
    print(f"Extracting {enums}")
    with open(file_name, "r") as f:
        lines = f.readlines()
        content = "\n".join(lines)

        enum_defs = []
        for enum_type_name in enums:
            enum_def = _get_enum_def(enum_type_name, content)
            human_name = _get_human_enum_name(enum_type_name)
            if enum_def is not None:
                description = _get_enum_description(enum_type_name, lines)
                enum_defs.append(
                    MetricType(
                        name=enum_type_name,
                        human_name=human_name,
                        description=description,
                        metrics=enum_def,
                    )
                )
    return enum_defs


def _get_human_enum_name(enum_name):
    enum_name_regex = "^Relay(.*)s$"
    match = re.search(enum_name_regex, enum_name)
    if match:
        return match.group(1)
    return enum_name


def _get_enum_description(enum_name, lines):
    enum_def_start_regex = fr"{enum_name}"

    line_start = None
    for idx, line in enumerate(lines):
        if re.search(enum_def_start_regex, line) is not None:
            line_start = idx
            break

    enum_description = []
    if line_start is not None:
        for line in reversed(lines[:line_start]):
            comment = _get_comment(line)
            if comment is not None:
                enum_description.append(comment)
            else:
                break
        enum_description.reverse()
    return enum_description


cfg_feature_extractor = re.compile(
    r"#\[cfg\(feature\s*=\s*\"(?P<feature_name>[^\"]*)\"\)\]"
)


def _get_feature(line):
    """
    Returns the features inside a
    :param line: the line to be parsed
    :return: a list

    >>> _get_feature('#[cfg(feature = "processing")]')
    'processing'
    >>> _get_feature('some random line')
    >>> _get_feature('#[cfg(not_a_feature = "processing")]')

    """
    result = cfg_feature_extractor.search(line)
    if result is None:
        return None
    return result.groupdict().get("feature_name")


def _get_enum_def(enum_type_name, lines):
    """
    Returns the contents of an enum
    """
    enum_regex_str = r"enum\s+" + enum_type_name + r"\s+\{(?P<enum_content>[^}]+)\}"
    result = re.search(enum_regex_str, lines, flags=re.MULTILINE)
    if result is None:
        return None

    inner_content = result.groupdict().get("enum_content")
    back_to_lines = inner_content.split("\n")
    enum_defs = []
    current_def = _new_enum_def()
    for line in back_to_lines:
        feature = _get_feature(line)

        if feature is not None:
            print(f"Detected feature {feature}.")
            current_def.feature = feature
        comment = _get_comment(line)
        if comment is not None:
            current_def.description.append(comment)
        else:
            enum_name = _get_enum_name(line)
            if len(enum_name) == 0 or enum_name.isspace():
                continue
            current_def.id = _get_metric_id_enum(enum_type_name, enum_name, lines)
            current_def.name = enum_name
            enum_defs.append(current_def)
            current_def = _new_enum_def()

    return enum_defs


def _get_metric_id_enum(enum_type, enum_member, lines):
    metric_name_regex = rf"{enum_type}::{enum_member}\s*=>\s*\"([^\"]*)\""
    search_result = re.search(metric_name_regex, lines, flags=re.MULTILINE)
    if search_result is not None:
        return search_result.group(1)


def _new_enum_def():
    return Metric(name=None, id=None, feature=None, description=[])


enum_doc_comment = re.compile(r"^\s*/{3,50}\s*(.*)")


def _get_comment(line):
    result = enum_doc_comment.match(line)
    if result is not None:
        return result.group(1)
    return None


enum_name = re.compile(r"\s*(\w+)")


def _get_enum_name(line):
    result = enum_name.match(line)
    if result is not None:
        return result.group(1)
    return ""


def _get_file_system_loader():
    file_loader = FileSystemLoader("doc_templates")
    return file_loader


def metrics_to_markdown(metrics, config):
    file_path = _get_file_path(config)
    env = Environment(loader=_get_file_system_loader())
    template = env.get_template("metrics.jinja.md")
    document_lines = template.generate(all_metrics=metrics)
    if document_lines is not None:
        with open(file_path, "w") as f:
            f.writelines(document_lines)


processors = [
    CommandDef(name="extract_metrics", fn=extract_metrics),
]

formatters = [
    CommandDef(name="metrics_to_markdown", fn=metrics_to_markdown),
]

if __name__ == "__main__":
    print("Extracting file info")
    file_defs = {fd.name: _get_file_path(fd.path) for fd in files}
    processor_defs = {p.name: p.fn for p in processors}
    formatter_defs = {f.name: f.fn for f in formatters}
    for extraction in extractions:
        try:
            file_name = extraction.file
            processor_name = extraction.processor
            processor_config = extraction.processor_config
            formatter_name = extraction.formatter
            formatter_config = extraction.formatter_config

            file_path = file_defs[file_name]
            processor = processor_defs[processor_name]
            formatter = formatter_defs[formatter_name]

            processing_result = processor(file_path, processor_config)
            result = formatter(processing_result, formatter_config)
        except Exception as ex:
            print(f"Failed to process {extraction}, with exception:\n {ex}")
            continue
