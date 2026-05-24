import os

import dlt
from source import example_source


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="example",
        destination="filesystem",
        dataset_name="example",
    )

    pipeline.run(example_source())


if __name__ == "__main__":
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    run()
