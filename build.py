import build
import os
import zlib

build.flags.allow({
    "coverage": {
        "descr": "instrument the binary; `./build -Dcoverage coverage` writes $(B)/coverage.out",
        "default": "",
    },
    "race": {
        "descr": "build s3 with the Go race detector; run with `./build -Drace test`",
        "default": "",
    },
})

COVERAGE = bool(build.flags.coverage)
RACE = bool(build.flags.race)


def mkdir(path):
    return [
        "python3",
        "-c",
        f"from pathlib import Path; Path(r'{path}').mkdir(parents=True, exist_ok=True)",
    ]


def touch(path):
    return [
        "python3",
        "-c",
        f"from pathlib import Path; p=Path(r'{path}'); p.parent.mkdir(parents=True, exist_ok=True); p.touch()",
    ]


GO_SOURCES = [
    path for path in build.glob("$(S)/*.go")
    if not path.endswith("_test.go")
]
GO_INPUTS = [
    *GO_SOURCES,
    "$(S)/go.mod",
    "$(S)/go.sum",
]

GO_ENV = {
    "CGO_ENABLED": "1" if RACE else "0",
    "GOFLAGS": "-mod=readonly -buildvcs=false",
    "GOTOOLCHAIN": "local",
    "GOWORK": "off",
}

# With -Dcoverage the binary counts what it executes (Go's own
# instrumentation) and every process writes its counters to GOCOVERDIR at
# exit, so the end-to-end tests measure coverage of the real program.
s3 = command(
    name="s3",
    inputs=GO_INPUTS,
    outputs=["$(B)/bin/s3"],
    cmd=[
        "go", "build",
        "-trimpath",
        "-buildvcs=false",
        *(["-race"] if RACE else []),
        *(["-cover", "-covermode=atomic"] if COVERAGE else []),
        "-o", "$(B)/bin/s3",
        ".",
    ],
    cwd="$(S)",
    env=GO_ENV,
    descr="GO",
    color="cyan",
)

# The same binary, built to be refused by the operating system now and then.
chaos_binary = command(
    name="chaos-binary",
    inputs=GO_INPUTS,
    outputs=["$(B)/bin/s3-chaos"],
    cmd=[
        "go", "build",
        "-trimpath",
        "-buildvcs=false",
        "-tags=s3chaos",
        *(["-cover", "-covermode=atomic"] if COVERAGE else []),
        "-o", "$(B)/bin/s3-chaos",
        ".",
    ],
    cwd="$(S)",
    env=GO_ENV,
    descr="GO",
    color="cyan",
)

# How often each point is refused, as one call in so many. Only the points
# every scenario survives unchanged are armed here: an accept refused is a
# connection served a moment later, a link that breaks is redialled and its
# table sent again. A dial that fails takes that table down with it, and the
# cell's side of a connection is also what the raw clients of the cell
# scenarios hold, so those points are armed in tst/test_chaos.py alone.
CHAOS_POINTS = ",".join([
    "accept:5",
    "link read:200",
    "link write:200",
])

e2e_tests = []
chaos_tests = []
coverage_dirs = []
chaos_coverage_dirs = []
for test_path in build.glob("$(S)/tst/test_*.py"):
    test_name = test_path.rsplit("/", 1)[-1][len("test_"):-len(".py")]
    test_stamp = f"$(B)/tests/{test_name}.stamp"
    env = {
        "S3_TEST_BINARY": s3.outputs[0],
        "S3_TEST_ARTIFACTS": os.environ.get("S3_TEST_ARTIFACTS", ""),
        "S3_TEST_ETCD": os.environ.get("S3_TEST_ETCD", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    prelude = []
    outputs = [test_stamp]
    inputs = [test_path, "$(S)/tst/lib.py"]

    if RACE:
        env["GORACE"] = "halt_on_error=1 atexit_sleep_ms=0"

    if COVERAGE:
        # the counters are a declared output so the coverage node sees them
        env["GOCOVERDIR"] = f"$(B)/coverage/{test_name}"
        prelude = [mkdir(env["GOCOVERDIR"])]
        coverage_dirs.append(env["GOCOVERDIR"])
        outputs.append(env["GOCOVERDIR"])

    e2e_tests.append(command(
        name=f"e2e_{test_name}",
        inputs=inputs,
        outputs=outputs,
        deps=[s3],
        cmd=[
            *prelude,
            ["python3", test_path],
            touch(test_stamp),
        ],
        cwd="$(S)",
        env=env,
        descr="EE",
        color="green",
    ))

    # The same scenario against a binary the kernel refuses now and then. The
    # seed comes from the name, so a point that breaks breaks again on a rerun.
    chaos_stamp = f"$(B)/chaos/{test_name}.stamp"
    chaos_env = {
        **{k: v for k, v in env.items() if k != "GOCOVERDIR"},
        "S3_TEST_BINARY": chaos_binary.outputs[0],
        "S3_CHAOS": CHAOS_POINTS,
        "S3_CHAOS_SEED": str(zlib.crc32(test_name.encode()) % 100000),
    }
    chaos_outputs = [chaos_stamp]
    chaos_prelude = []

    # What the refusals walk is exactly what the plain suite cannot reach, so
    # these counters are worth keeping apart and reading on their own.
    if COVERAGE:
        chaos_env["GOCOVERDIR"] = f"$(B)/coverage-chaos/{test_name}"
        chaos_prelude = [mkdir(chaos_env["GOCOVERDIR"])]
        chaos_coverage_dirs.append(chaos_env["GOCOVERDIR"])
        chaos_outputs.append(chaos_env["GOCOVERDIR"])

    chaos_tests.append(command(
        name=f"chaos_{test_name}",
        inputs=inputs,
        outputs=chaos_outputs,
        deps=[chaos_binary],
        cmd=[
            *chaos_prelude,
            ["python3", test_path],
            touch(chaos_stamp),
        ],
        cwd="$(S)",
        env=chaos_env,
        descr="KO",
        color="red",
    ))

# A point that is armed but never consulted refuses nothing and says nothing.
chaos_points = command(
    name="chaos-points",
    inputs=[*GO_SOURCES, "$(S)/dev/chaos_points.py"],
    outputs=["$(B)/chaos-points.stamp"],
    cmd=[
        ["python3", "$(S)/dev/chaos_points.py"],
        touch("$(B)/chaos-points.stamp"),
    ],
    cwd="$(S)",
    descr="KO",
    color="red",
)

group("install", s3)
group("e2e", *e2e_tests)
group("test", *e2e_tests)
group("chaos", chaos_points, *chaos_tests)

if COVERAGE:
    chaos_coverage = command(
        name="coverage-chaos",
        inputs=["$(S)/dev/coverage.py"],
        outputs=["$(B)/coverage-chaos.out"],
        deps=chaos_tests,
        cmd=["python3", "$(S)/dev/coverage.py", "--output", "$(B)/coverage-chaos.out", "--minimum", "0", *chaos_coverage_dirs],
        cwd="$(S)",
        env=GO_ENV,
        descr="CV",
        color="magenta",
    )

    coverage = command(
        name="coverage",
        inputs=["$(S)/dev/coverage.py"],
        outputs=["$(B)/coverage.out"],
        deps=e2e_tests,
        cmd=["python3", "$(S)/dev/coverage.py", "--output", "$(B)/coverage.out", "--minimum", "85", *coverage_dirs],
        cwd="$(S)",
        env=GO_ENV,
        descr="CV",
        color="magenta",
    )
