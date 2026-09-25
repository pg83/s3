import build
import os

build.flags.allow({
    "race": {
        "descr": "build s3 with the Go race detector; run with `./build -Drace test`",
        "default": "",
    },
})

RACE = bool(build.flags.race)


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

s3 = command(
    name="s3",
    inputs=GO_INPUTS,
    outputs=["$(B)/bin/s3"],
    cmd=[
        "go", "build",
        "-trimpath",
        "-buildvcs=false",
        *(["-race"] if RACE else []),
        "-o", "$(B)/bin/s3",
        ".",
    ],
    cwd="$(S)",
    env=GO_ENV,
    descr="GO",
    color="cyan",
)

e2e_tests = []
for test_path in build.glob("$(S)/tst/test_*.py"):
    test_name = test_path.rsplit("/", 1)[-1][len("test_"):-len(".py")]
    test_stamp = f"$(B)/tests/{test_name}.stamp"
    env = {
        "S3_TEST_BINARY": s3.outputs[0],
        "S3_TEST_ARTIFACTS": os.environ.get("S3_TEST_ARTIFACTS", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
    }

    if RACE:
        env["GORACE"] = "halt_on_error=1 atexit_sleep_ms=0"

    e2e_tests.append(command(
        name=f"e2e_{test_name}",
        inputs=[test_path, "$(S)/tst/lib.py"],
        outputs=[test_stamp],
        deps=[s3],
        cmd=[
            ["python3", test_path],
            touch(test_stamp),
        ],
        cwd="$(S)",
        env=env,
        descr="EE",
        color="green",
    ))

group("install", s3)
group("e2e", *e2e_tests)
group("test", *e2e_tests)
