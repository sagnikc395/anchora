from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

from anchora.agents import build_workflow
from anchora.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="anchora",
        description="Run the Anchora smolagents workflow.",
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to the YAML config file.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not load_dotenv():
        logging.info("no .env file found, falling back to system env")

    config = load_config(Path(args.config))
    workflow = build_workflow(config)
    workflow.run()


if __name__ == "__main__":
    main()
