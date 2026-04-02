import argparse
import logging
from pathlib import Path

from schema_optimizer import optimize_schema_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optimize a large schema cache JSON for AI-friendly lookup usage.")
    parser.add_argument(
        "input",
        nargs="?",
        default="schema_cache.json",
        help="Path to the legacy schema JSON file.",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default="schema_cache.optimized.json",
        help="Path to the optimized output JSON file.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=262_144,
        help="Streaming read chunk size in bytes.",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    logger = logging.getLogger("optimize_schema")

    input_path = Path(args.input)
    output_path = Path(args.output)
    optimize_schema_file(
        input_path=input_path,
        output_path=output_path,
        chunk_size=args.chunk_size,
        logger=logger,
    )
    logger.info("Optimized schema written to %s", output_path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
