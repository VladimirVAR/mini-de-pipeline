from dataclasses import dataclass
import os

from dotenv import load_dotenv


SUPPORTED_LOAD_MODES = {"full_refresh", "append"}
SUPPORTED_LOAD_METHODS = {"executemany", "copy"}


@dataclass(frozen=True)
class PipelineConfig:
    load_mode: str
    load_method: str


def get_pipeline_config() -> PipelineConfig:
    load_dotenv()

    load_mode = os.getenv("PIPELINE_LOAD_MODE", "full_refresh").strip().lower()
    load_method = os.getenv("PIPELINE_LOAD_METHOD", "executemany").strip().lower()

    if load_mode not in SUPPORTED_LOAD_MODES:
        raise ValueError(
            f"Unsupported PIPELINE_LOAD_MODE: {load_mode}. "
            f"Supported values: {sorted(SUPPORTED_LOAD_MODES)}"
        )

    if load_method not in SUPPORTED_LOAD_METHODS:
        raise ValueError(
            f"Unsupported PIPELINE_LOAD_METHOD: {load_method}. "
            f"Supported values: {sorted(SUPPORTED_LOAD_METHODS)}"
        )

    return PipelineConfig(load_mode=load_mode, load_method=load_method)