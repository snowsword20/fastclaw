#!/usr/bin/env python3
"""Generate random arithmetic practice problems, or solve submitted ones.

Run from the skill root:
    python3 scripts/gen_problems.py --count 5 --a-digits 2 --b-digits 2 --op mul
    python3 scripts/gen_problems.py --solve "47*63" "56*54"
"""

import argparse
import random
import re
import sys

OPS = {
    "mul": "×",
    "div": "÷",
    "add": "+",
    "sub": "−",
}


def bounds(digits: int) -> tuple[int, int]:
    digits = max(1, min(digits, 6))
    return (10 ** (digits - 1), 10**digits - 1)


def make_problem(rng: random.Random, op: str, ad: int, bd: int) -> tuple[int, str, int, int]:
    a_lo, a_hi = bounds(ad)
    b_lo, b_hi = bounds(bd)
    a = rng.randint(a_lo, a_hi)
    b = rng.randint(b_lo, b_hi)
    if op == "mul":
        return (a, OPS[op], b, a * b)
    if op == "add":
        return (a, OPS[op], b, a + b)
    if op == "sub":
        if a < b:
            a, b = b, a  # practice sheets stay non-negative
        return (a, OPS[op], b, a - b)
    # div: build dividend from divisor × quotient so it divides exactly;
    # a-digits controls the quotient, b-digits the divisor.
    quotient = a
    return (quotient * b, OPS[op], b, quotient)


def generate(args) -> list[tuple[int, str, int, int]]:
    rng = random.Random(args.seed)
    seen: set[tuple[int, int]] = set()
    problems: list[tuple[int, str, int, int]] = []
    attempts = 0
    while len(problems) < args.count and attempts < args.count * 100:
        attempts += 1
        p = make_problem(rng, args.op, args.a_digits, args.b_digits)
        key = (p[0], p[2])
        if key in seen:
            continue
        seen.add(key)
        problems.append(p)
    if len(problems) < args.count:
        print(f"warning: only {len(problems)} unique problems possible for this shape", file=sys.stderr)
    return problems


ALLOWED = re.compile(r"^[0-9+\-*/×÷xX(). ]+$")


def solve(expressions: list[str]) -> None:
    for raw in expressions:
        expr = raw.strip()
        if not expr or not ALLOWED.match(expr):
            print(f"{raw} = <invalid expression>")
            continue
        normalized = expr.replace("×", "*").replace("x", "*").replace("X", "*").replace("÷", "/").replace("−", "-")
        try:
            result = eval(normalized, {"__builtins__": {}}, {})  # charset-guarded above
        except ZeroDivisionError:
            print(f"{expr} = <division by zero>")
            continue
        except Exception:
            print(f"{expr} = <invalid expression>")
            continue
        if isinstance(result, float) and result.is_integer():
            result = int(result)
        print(f"{expr.replace('*', '×').replace('/', '÷')} = {result}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate arithmetic practice problems")
    parser.add_argument("--count", type=int, default=5, help="number of problems (default 5)")
    parser.add_argument("--op", choices=sorted(OPS), default="mul", help="operation (default mul)")
    parser.add_argument("--a-digits", type=int, default=2, help="left operand digits; for div, quotient digits")
    parser.add_argument("--b-digits", type=int, default=2, help="right operand digits; for div, divisor digits")
    parser.add_argument("--seed", type=int, default=None, help="same seed reproduces the same problems")
    parser.add_argument("--answers", action="store_true", help="append an answer key")
    parser.add_argument("--solve", nargs="+", metavar="EXPR", help="solve given expressions instead of generating")
    args = parser.parse_args()

    if args.solve:
        solve(args.solve)
        return

    problems = generate(args)
    for i, (a, sym, b, _) in enumerate(problems, 1):
        print(f"{i}. {a} {sym} {b} = ____")
    if args.answers:
        print()
        print("答案：")
        for i, (a, sym, b, ans) in enumerate(problems, 1):
            print(f"{i}. {a} {sym} {b} = {ans}")


if __name__ == "__main__":
    main()
