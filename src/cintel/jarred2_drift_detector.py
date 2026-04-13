"""
case_drift_detector.py - Project script (example).
Modified for Severity Analysis and Drift Counting.
"""

# === DECLARE IMPORTS ===

import logging
from pathlib import Path
from typing import Final

import polars as pl
from datafun_toolkit.logger import get_logger, log_header

# === CONFIGURE LOGGER ===

LOG: logging.Logger = get_logger("P5", level="DEBUG")

# === DEFINE GLOBAL PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

REFERENCE_FILE: Final[Path] = DATA_DIR / "reference_metrics_case.csv"
CURRENT_FILE: Final[Path] = DATA_DIR / "current_metrics_case.csv"

OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "drift_summary_jarred2.csv"
SUMMARY_LONG_FILE: Final[Path] = ARTIFACTS_DIR / "drift_summary_long_jarred2.csv"

# === DEFINE THRESHOLDS ===

REQUESTS_DRIFT_THRESHOLD: Final[float] = 20.0
ERRORS_DRIFT_THRESHOLD: Final[float] = 2.0
LATENCY_DRIFT_THRESHOLD: Final[float] = 1000.0
CRITICAL_THRESHOLD_PCT: Final[float] = 50.0

# === DEFINE THE MAIN FUNCTION ===


def main() -> None:
    """Run the pipeline."""
    log_header(LOG, "CINTEL")

    LOG.info("START main()")

    # Ensure the artifacts folder exists.
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------
    # STEP 1: READ REFERENCE AND CURRENT CSV INTO DATAFRAMES
    # ----------------------------------------------------
    reference_df = pl.read_csv(REFERENCE_FILE)
    current_df = pl.read_csv(CURRENT_FILE)

    LOG.info(f"Loaded {reference_df.height} reference records")
    LOG.info(f"Loaded {current_df.height} current records")

    # ----------------------------------------------------
    # STEP 2: CALCULATE AVERAGE METRICS FOR EACH PERIOD
    # ----------------------------------------------------
    reference_summary_df = reference_df.select(
        [
            pl.col("requests").mean().alias("reference_avg_requests"),
            pl.col("errors").mean().alias("reference_avg_errors"),
            pl.col("total_latency_ms").mean().alias("reference_avg_latency_ms"),
        ]
    )

    current_summary_df = current_df.select(
        [
            pl.col("requests").mean().alias("current_avg_requests"),
            pl.col("errors").mean().alias("current_avg_errors"),
            pl.col("total_latency_ms").mean().alias("current_avg_latency_ms"),
        ]
    )

    # ----------------------------------------------------
    # STEP 3: COMBINE THE TWO ONE-ROW SUMMARY TABLES
    # ----------------------------------------------------
    combined_df: pl.DataFrame = pl.concat(
        [reference_summary_df, current_summary_df],
        how="horizontal",
    )

    # ----------------------------------------------------
    # STEP 4: CALCULATE PERCENTAGE AND RAW DIFFERENCE
    # ----------------------------------------------------
    # Defining recipes for raw differences
    requests_diff_recipe = (
        pl.col("current_avg_requests") - pl.col("reference_avg_requests")
    ).alias("requests_mean_difference")
    errors_diff_recipe = (
        pl.col("current_avg_errors") - pl.col("reference_avg_errors")
    ).alias("errors_mean_difference")
    latency_diff_recipe = (
        pl.col("current_avg_latency_ms") - pl.col("reference_avg_latency_ms")
    ).alias("latency_mean_difference_ms")

    # Defining recipes for percentage changes
    requests_pct_recipe = (
        (
            (
                (pl.col("current_avg_requests") - pl.col("reference_avg_requests"))
                / pl.col("reference_avg_requests")
            )
            * 100
        )
        .round(2)
        .alias("requests_pct_change")
    )
    errors_pct_recipe = (
        (
            (
                (pl.col("current_avg_errors") - pl.col("reference_avg_errors"))
                / pl.col("reference_avg_errors")
            )
            * 100
        )
        .round(2)
        .alias("errors_pct_change")
    )
    latency_pct_recipe = (
        (
            (
                (pl.col("current_avg_latency_ms") - pl.col("reference_avg_latency_ms"))
                / pl.col("reference_avg_latency_ms")
            )
            * 100
        )
        .round(2)
        .alias("latency_pct_change")
    )

    drift_df = combined_df.with_columns(
        [
            requests_diff_recipe,
            errors_diff_recipe,
            latency_diff_recipe,
            requests_pct_recipe,
            errors_pct_recipe,
            latency_pct_recipe,
        ]
    )

    # ----------------------------------------------------
    # STEP 5: DEFINE DRIFT AND SEVERITY FLAGS
    # ----------------------------------------------------
    drift_df = drift_df.with_columns(
        [
            (pl.col("requests_mean_difference").abs() > REQUESTS_DRIFT_THRESHOLD).alias(
                "requests_is_drifting_flag"
            ),
            (pl.col("errors_mean_difference").abs() > ERRORS_DRIFT_THRESHOLD).alias(
                "errors_is_drifting_flag"
            ),
            (
                pl.col("latency_mean_difference_ms").abs() > LATENCY_DRIFT_THRESHOLD
            ).alias("latency_is_drifting_flag"),
            (pl.col("requests_pct_change").abs() > CRITICAL_THRESHOLD_PCT).alias(
                "requests_is_critical"
            ),
            (pl.col("errors_pct_change").abs() > CRITICAL_THRESHOLD_PCT).alias(
                "errors_is_critical"
            ),
            (pl.col("latency_pct_change").abs() > CRITICAL_THRESHOLD_PCT).alias(
                "latency_is_critical"
            ),
        ]
    )

    # ----------------------------------------------------
    # STEP 5.1: CALCULATE TOTAL DRIFT COUNT
    # ----------------------------------------------------
    drift_df = drift_df.with_columns(
        (
            pl.col("requests_is_drifting_flag").cast(pl.Int8)
            + pl.col("errors_is_drifting_flag").cast(pl.Int8)
            + pl.col("latency_is_drifting_flag").cast(pl.Int8)
        ).alias("total_drift_count")
    )

    LOG.info("Calculated summary differences, percentages, and drift flags")

    # ----------------------------------------------------
    # STEP 6: SAVE AND LOG RESULTS
    # ----------------------------------------------------
    drift_df.write_csv(OUTPUT_FILE)
    LOG.info(f"Wrote drift summary file: {OUTPUT_FILE}")

    # Convert to dictionary for easier logging
    drift_summary_dict = drift_df.to_dicts()[0]

    LOG.info("Drift summary (one field per line):")
    for field_name, field_value in drift_summary_dict.items():
        LOG.info(f"{field_name}: {field_value}")

    # Create long-form artifact
    drift_summary_long_df = pl.DataFrame(
        {
            "field_name": list(drift_summary_dict.keys()),
            "field_value": [str(value) for value in drift_summary_dict.values()],
        }
    )

    drift_summary_long_df.write_csv(SUMMARY_LONG_FILE)
    LOG.info(f"Wrote long summary file: {SUMMARY_LONG_FILE}")
    LOG.info("END main()")


if __name__ == "__main__":
    main()
