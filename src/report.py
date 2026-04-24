"""
=============================================================
Report Generator — CLI + HTML summaries
=============================================================
"""

from datetime import datetime


class ReportGenerator:
    def print_summary(self, health) -> None:
        sep = "─" * 60
        status_icon = {"OK": "✅", "WARNING": "⚠️", "CRITICAL": "🔴"}.get(health.overall_status, "❓")

        print(f"\n{sep}")
        print(f"  PIPELINE HEALTH REPORT")
        print(sep)
        print(f"  Pipeline : {health.pipeline_name}")
        print(f"  Table    : {health.table_name}")
        print(f"  Run at   : {health.run_at}")
        print(f"  Rows     : {health.total_rows:,}")
        print(f"  Status   : {status_icon} {health.overall_status}")
        print(f"  Passed   : {health.checks_passed} checks")
        print(f"  Failed   : {health.checks_failed} checks")

        if health.issues:
            print(f"\n  Issues:")
            for issue in health.issues:
                sev = "🔴" if issue["severity"] == "CRITICAL" else "🟡"
                print(f"    {sev} [{issue['check']}] {issue['column']} — {issue['detail']}")
        else:
            print("\n  ✅ No issues found. Data is reliable.")
        print(f"{sep}\n")
