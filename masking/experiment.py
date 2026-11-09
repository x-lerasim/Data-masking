from __future__ import annotations

import csv
import sys
from collections import Counter

from masking.db import get_client
from masking.masker import _render, _rule_params, rules

K_VALUES = (3, 5, 10)
PERIODS = {"connected_quarter": "квартал", "connected_half": "полугодие"}


def load_clients() -> list[dict]:
    res = get_client().query(_render("experiment_k.sql"), parameters=_rule_params(0))
    return [dict(zip(res.column_names, r)) for r in res.result_rows]


def scenario(clients: list[dict], period: str, k_min: int) -> dict:
    qi = lambda c: (c["federal_district"], c["rooms_range"], c[period])
    sizes = Counter(qi(c) for c in clients)
    kept = [c for c in clients if sizes[qi(c)] >= k_min]
    kept_sizes = [n for n in sizes.values() if n >= k_min]
    rows_all = sum(c["fact_rows"] for c in clients)
    rows_kept = sum(c["fact_rows"] for c in kept)
    return {
        "scenario": f"{PERIODS[period]}, K_MIN={k_min}",
        "period": PERIODS[period],
        "k_min": k_min,
        "groups_before": len(sizes),
        "groups_after": len(kept_sizes),
        "k_actual": min(kept_sizes) if kept_sizes else 0,
        "clients_before": len(clients),
        "clients_after": len(kept),
        "clients_suppressed": len(clients) - len(kept),
        "clients_suppressed_pct": round(100 * (len(clients) - len(kept)) / len(clients), 1),
        "fact_rows_before": rows_all,
        "fact_rows_after": rows_kept,
        "fact_rows_suppressed": rows_all - rows_kept,
        "fact_rows_suppressed_pct": round(100 * (rows_all - rows_kept) / rows_all, 1),
        "churn_clients_pct_before": round(100 * sum(c["churn_rows"] > 0 for c in clients) / len(clients), 1),
        "churn_clients_pct_after": round(100 * sum(c["churn_rows"] > 0 for c in kept) / max(len(kept), 1), 1),
        "churn_rows_pct_before": round(100 * sum(c["churn_rows"] for c in clients) / rows_all, 2),
        "churn_rows_pct_after": round(100 * sum(c["churn_rows"] for c in kept) / max(rows_kept, 1), 2),
        "by_district": Counter(c["federal_district"] for c in kept),
        "by_rooms": Counter(c["rooms_range"] for c in kept),
    }


def run() -> tuple[list[dict], list[dict]]:
    clients = load_clients()
    results = [scenario(clients, p, k) for p in PERIODS for k in K_VALUES]
    base = {"by_district": Counter(c["federal_district"] for c in clients),
            "by_rooms": Counter(c["rooms_range"] for c in clients)}
    return results, [base]


def districts() -> list[str]:
    return list(dict.fromkeys(rules()["city_to_district"].values()))


def rooms() -> list[str]:
    return rules()["rooms_ranges"]["labels"]


def write_csv(results: list[dict], base: dict, out) -> None:
    w = csv.writer(out)
    w.writerow(["scenario", "metric", "value"])
    for d in districts():
        w.writerow(["dwh", f"district:{d}", base["by_district"].get(d, 0)])
    for r in rooms():
        w.writerow(["dwh", f"rooms:{r}", base["by_rooms"].get(r, 0)])
    for res in results:
        for key, val in res.items():
            if key in ("scenario", "by_district", "by_rooms"):
                continue
            w.writerow([res["scenario"], key, val])
        for d in districts():
            w.writerow([res["scenario"], f"district:{d}", res["by_district"].get(d, 0)])
        for r in rooms():
            w.writerow([res["scenario"], f"rooms:{r}", res["by_rooms"].get(r, 0)])


def write_md(results: list[dict], base: dict, out) -> None:
    p = lambda s: out.write(s + "\n")
    names = [r["scenario"] for r in results]
    p("| Показатель | " + " | ".join(names) + " |")
    p("|---" * (len(names) + 1) + "|")
    rows = [
        ("Групп по КИ в dwh", "groups_before"),
        ("Групп в ml", "groups_after"),
        ("Фактическое k", "k_actual"),
        ("Клиентов в ml", "clients_after"),
        ("Подавлено клиентов", "clients_suppressed"),
        ("Подавлено клиентов, %", "clients_suppressed_pct"),
        ("Строк факта в ml", "fact_rows_after"),
        ("Подавлено строк факта", "fact_rows_suppressed"),
        ("Подавлено строк факта, %", "fact_rows_suppressed_pct"),
        ("Доля ушедших клиентов до, %", "churn_clients_pct_before"),
        ("Доля ушедших клиентов после, %", "churn_clients_pct_after"),
        ("Доля строк churn_flag=1 до, %", "churn_rows_pct_before"),
        ("Доля строк churn_flag=1 после, %", "churn_rows_pct_after"),
    ]
    for title, key in rows:
        p(f"| {title} | " + " | ".join(str(r[key]) for r in results) + " |")
    p("")
    for title, key, labels in (("Федеральный округ", "by_district", districts()),
                               ("rooms_range", "by_rooms", rooms())):
        p(f"| {title} | dwh | " + " | ".join(names) + " |")
        p("|---" * (len(names) + 2) + "|")
        for label in labels:
            p(f"| {label} | {base[key].get(label, 0)} | " + " | ".join(str(r[key].get(label, 0)) for r in results) + " |")
        p("")


def main() -> None:
    fmt = sys.argv[1] if len(sys.argv) > 1 else "md"
    results, (base,) = run()
    (write_csv if fmt == "csv" else write_md)(results, base, sys.stdout)


if __name__ == "__main__":
    main()
