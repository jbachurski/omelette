import string
import subprocess
import statistics


def duration_ms(s: str) -> float:
    unit = s.lstrip(string.digits + ".")
    return {"ms": 1, "s": 1000}[unit] * float(s[: -len(unit)])


def expr(n):
    return "(. 1)" if n <= 1 else f"(+ (. {n}) {expr(n - 1)})"


def run(n, w):
    return duration_ms(
        subprocess.run(
            ["cargo", "run", "--release", "--", f"{expr(n)}", "-w", f"{w}"],
            capture_output=True,
        )
        .stdout.splitlines()[-1]
        .decode()
        .strip()
    )


for w in [1, 2, 4, 8]:
    print(f"=== {w = } ===")
    print("x\ty\tstd")
    for n in range(1, 10):
        ts = [run(n, w) / 1000 for _ in range(5)]
        print(f"{n}\t{statistics.mean(ts):.3f}\t{3*statistics.pstdev(ts):.3f}")
