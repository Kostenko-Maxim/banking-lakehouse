import argparse
import os
from datetime import date, timedelta

from bank.config import DATA
from bank.generator import PROFILES, generate


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("init", "demo", "schema-demo", "maintenance"):
        commands.add_parser(name)
    gen = commands.add_parser("generate")
    gen.add_argument("--date", required=True)
    gen.add_argument("--profile", choices=PROFILES, default="small")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument(
        "--scenario",
        choices=[
            "initial",
            "daily",
            "duplicates",
            "correction",
            "cancel",
            "late",
            "customer-late",
            "invalid",
        ],
        default="initial",
    )
    run = commands.add_parser("run")
    run.add_argument("batch_id")
    run.add_argument("--run-id")
    run.add_argument("--failure", choices=["before-publish", "inside-publish", "reconcile"])
    fill = commands.add_parser("backfill")
    fill.add_argument("--start", required=True)
    fill.add_argument("--end", required=True)
    bench = commands.add_parser("benchmark")
    bench.add_argument("--date", required=True)
    bench.add_argument("--profile", choices=PROFILES, default="1m")
    bench.add_argument("--repeats", type=int, default=3)
    freshness = commands.add_parser("freshness")
    freshness.add_argument("--max-age", type=int, default=int(os.getenv("FRESHNESS_SECONDS", "86400")))
    args = parser.parse_args()
    if args.command == "generate":
        print(generate(DATA / "sources", args.date, args.profile, args.seed, args.scenario).name)
    elif args.command == "init":
        from bank.pipeline import init

        init()
    elif args.command == "run":
        from bank.pipeline import run

        run(DATA / "sources" / args.batch_id, args.failure, args.run_id)
    elif args.command == "demo":
        from bank.demo import demo

        demo()
    elif args.command == "backfill":
        from bank.pipeline import run

        start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
        if start > end:
            parser.error("START must be <= END")
        while start <= end:
            run(generate(DATA / "sources", start, scenario="daily"))
            start += timedelta(days=1)
    elif args.command in ("schema-demo", "maintenance"):
        from bank.pipeline import spark_job, writer

        with writer() as pg:
            if pg.execute("SELECT count(*) FROM pipeline_lock").fetchone()[0]:
                raise RuntimeError("An unfinished pipeline owns the tables")
            print(spark_job(args.command))
    elif args.command == "benchmark":
        from bank.benchmark import benchmark

        benchmark(args.date, args.profile, args.repeats)
    elif args.command == "freshness":
        from bank.alerts import check_freshness

        if not check_freshness(args.max_age):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
