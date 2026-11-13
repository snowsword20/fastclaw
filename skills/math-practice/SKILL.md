---
name: math-practice
description: Generate random arithmetic practice problems (multiplication, division, addition, subtraction) with configurable digit counts and grade them. Use whenever the user asks for math exercises, quiz problems, or mentions 出题、练习题、口算题、乘法题、数学题、批改 — e.g. "给我出5道两位数乘两位数的题目".
metadata:
  fastclaw:
    emoji: "🧮"
---

# Math Practice Problem Generator

Generate and grade arithmetic practice problems by running this skill's
script — never invent problems or compute products yourself; multi-digit
arithmetic done "in your head" is the #1 source of wrong answers.

## Generate Problems

```bash
python3 scripts/gen_problems.py --count 5 --a-digits 2 --b-digits 2 --op mul
```

| Flag | Default | Meaning |
|---|---|---|
| `--count` | `5` | Number of problems. When the user says 几道/一些 without a number, use 5. |
| `--op` | `mul` | `mul` `div` `add` `sub` (乘、除、加、减). |
| `--a-digits` | `2` | Digit count of the left operand (for `div`: the quotient). |
| `--b-digits` | `2` | Digit count of the right operand (for `div`: the divisor; division is always exact). |
| `--seed` | random | Pass the same seed to regenerate the same set. |
| `--answers` | off | Append an answer key. Use when the user asks for 参考答案/答案. |

Examples:

```bash
# 两位数乘一位数口算 10 道
python3 scripts/gen_problems.py --count 10 --a-digits 2 --b-digits 1 --op mul

# 三位数除以两位数（整除），带答案
python3 scripts/gen_problems.py --count 8 --a-digits 2 --b-digits 2 --op div --answers
```

## Grade Answers

When the user replies with their answers (e.g. "1. 2961 2. 3024 …"), check
them with `--solve` — do not multiply by hand:

```bash
python3 scripts/gen_problems.py --solve "47*63" "56*54"
```

Accept `*`, `×`, `x`, `÷`, `/`, `+`, `-`, spaces, and parentheses. It prints
one `expression = result` per line; compare against the user's answers and
report which are right or wrong.

## Rules

- Always execute the script via exec and paste its output as the basis of
  your reply; you may reformat (numbering, layout) but never change the
  numbers.
- The script guarantees no duplicate problems within one run.
- If the user asks for problems of a kind the script doesn't cover (fractions,
  negative results, mixed ops in one sheet), say so and offer the closest
  supported shape instead of hand-writing problems.
