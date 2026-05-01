import os
import sys
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/mplconfig")

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from quant_infra.const import RESID_REG_WINDOW, SPEC_VOL_WINDOW

DB_PATH = ROOT / "Data" / "data.db"
PROOF_DIR = ROOT / "proof"
OUTPUT_DIR = PROOF_DIR / "output"
FIG_DIR = OUTPUT_DIR / "figures"
TABLE_DIR = OUTPUT_DIR / "tables"

PERIODS = [
    ("2017-2019", 20170315, 20191231),
    ("2020-2022", 20200101, 20221231),
    ("2023-2026YTD", 20230101, 20260422),
]
SAMPLE_FILES = {
    "全市场": None,
    "中证800": ROOT / "Data" / "Metadata" / "000906.SH_ins.csv",
    "中证1000": ROOT / "Data" / "Metadata" / "000852.SH_ins.csv",
}
FACTOR_NAMES = ["MKT", "SMB", "HML", "UMD"]


def ensure_dirs():
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    TABLE_DIR.mkdir(parents=True, exist_ok=True)


def frame_text(df, index=False):
    return "```\n" + df.to_string(index=index) + "\n```"


def ols_with_stats(X, y):
    X_reg = np.column_stack([np.ones(len(X)), X])
    beta = np.linalg.lstsq(X_reg, y, rcond=None)[0]
    y_hat = X_reg @ beta
    resid = y - y_hat
    ss_res = float(np.sum(resid ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = np.nan if ss_tot <= 0 else 1 - ss_res / ss_tot
    return beta, y_hat, resid, r2


def load_base_data(conn):
    query = """
    WITH resid_universe AS (
        SELECT DISTINCT ts_code FROM stock_resids
    )
    SELECT
        b.ts_code,
        b.trade_date::INT AS trade_date,
        b.pct_chg::DOUBLE AS pct_chg,
        p.MKT::DOUBLE AS MKT,
        p.SMB::DOUBLE AS SMB,
        p.HML::DOUBLE AS HML,
        p.UMD::DOUBLE AS UMD
    FROM stock_bar b
    JOIN pricing_factors p
      ON b.trade_date = p.trade_date
    JOIN resid_universe u
      ON b.ts_code = u.ts_code
    ORDER BY b.ts_code, b.trade_date
    """
    df = conn.execute(query).fetchdf()
    for col in ["pct_chg", *FACTOR_NAMES]:
        df[col] = df[col].astype("float32")
    df["ts_code"] = df["ts_code"].astype("category")
    df["trade_date"] = df["trade_date"].astype("int32")
    return df


def load_rolling_resids(conn):
    df = conn.execute(
        """
        SELECT ts_code, trade_date::INT AS trade_date, resid::DOUBLE AS resid_rolling
        FROM stock_resids
        ORDER BY ts_code, trade_date
        """
    ).fetchdf()
    df["ts_code"] = df["ts_code"].astype("category")
    df["trade_date"] = df["trade_date"].astype("int32")
    df["resid_rolling"] = df["resid_rolling"].astype("float32")
    return df


def load_rolling_spec(conn):
    df = conn.execute(
        """
        SELECT ts_code, trade_date::INT AS trade_date, factor::DOUBLE AS factor_rolling
        FROM spec_vol
        ORDER BY ts_code, trade_date
        """
    ).fetchdf()
    df["ts_code"] = df["ts_code"].astype("category")
    df["trade_date"] = df["trade_date"].astype("int32")
    df["factor_rolling"] = df["factor_rolling"].astype("float32")
    return df


def compute_static_residuals(base_df):
    X_all = base_df[FACTOR_NAMES].to_numpy(dtype=np.float64, copy=False)
    y_all = base_df["pct_chg"].to_numpy(dtype=np.float64, copy=False)
    static_resid = np.empty(len(base_df), dtype=np.float32)
    group_indices = base_df.groupby("ts_code", observed=True, sort=False).indices

    for _, idx in tqdm(group_indices.items(), desc="Computing static residuals"):
        beta, y_hat, _, _ = ols_with_stats(X_all[idx], y_all[idx])
        static_resid[idx] = (y_all[idx] - y_hat).astype(np.float32)

    out = base_df[["ts_code", "trade_date"]].copy()
    out["resid_static"] = static_resid
    return out


def compute_static_spec(static_resids):
    factor = np.full(len(static_resids), np.nan, dtype=np.float32)
    group_indices = static_resids.groupby("ts_code", observed=True, sort=False).indices
    resid_values = static_resids["resid_static"].to_numpy(dtype=np.float64, copy=False)

    for _, idx in tqdm(group_indices.items(), desc="Computing static spec_vol"):
        rolled = pd.Series(resid_values[idx]).rolling(
            window=SPEC_VOL_WINDOW, min_periods=SPEC_VOL_WINDOW
        ).std()
        factor[idx] = rolled.to_numpy(dtype=np.float32)

    out = static_resids[["ts_code", "trade_date"]].copy()
    out["factor_static"] = factor
    return out.dropna(subset=["factor_static"]).reset_index(drop=True)


def experiment_residual_purity(base_df, static_resids, rolling_resids):
    aligned = (
        base_df[["ts_code", "trade_date", *FACTOR_NAMES]]
        .merge(static_resids, on=["ts_code", "trade_date"], how="inner")
        .merge(rolling_resids, on=["ts_code", "trade_date"], how="inner")
        .sort_values(["ts_code", "trade_date"])
        .reset_index(drop=True)
    )

    stats = []
    for period_name, start_d, end_d in PERIODS:
        sub = aligned[(aligned["trade_date"] >= start_d) & (aligned["trade_date"] <= end_d)].copy()
        X_sub = sub[FACTOR_NAMES].to_numpy(dtype=np.float64, copy=False)
        group_indices = sub.groupby("ts_code", observed=True, sort=False).indices

        for code, idx in tqdm(group_indices.items(), desc=f"Experiment 1: residual purity {period_name}"):
            if len(idx) < 120:
                continue
            for method in ["static", "rolling"]:
                y = sub[f"resid_{method}"].to_numpy(dtype=np.float64, copy=False)[idx]
                beta, _, _, r2 = ols_with_stats(X_sub[idx], y)
                stats.append(
                    {
                        "period": period_name,
                        "ts_code": str(code),
                        "method": method,
                        "obs": len(idx),
                        "r2": r2,
                        "abs_MKT": abs(beta[1]),
                        "abs_SMB": abs(beta[2]),
                        "abs_HML": abs(beta[3]),
                        "abs_UMD": abs(beta[4]),
                        "coef_l1": np.abs(beta[1:]).sum(),
                    }
                )

    stats_df = pd.DataFrame(stats)
    summary_by_period = (
        stats_df.groupby(["period", "method"])
        .agg(
            stocks=("ts_code", "count"),
            median_r2=("r2", "median"),
            mean_r2=("r2", "mean"),
            p90_r2=("r2", lambda x: x.quantile(0.9)),
            median_coef_l1=("coef_l1", "median"),
            median_abs_mkt=("abs_MKT", "median"),
            median_abs_smb=("abs_SMB", "median"),
            median_abs_hml=("abs_HML", "median"),
            median_abs_umd=("abs_UMD", "median"),
        )
        .reset_index()
    )
    summary = (
        stats_df.groupby("method")
        .agg(
            stocks=("ts_code", "count"),
            median_r2=("r2", "median"),
            mean_r2=("r2", "mean"),
            p90_r2=("r2", lambda x: x.quantile(0.9)),
            median_coef_l1=("coef_l1", "median"),
            median_abs_mkt=("abs_MKT", "median"),
            median_abs_smb=("abs_SMB", "median"),
            median_abs_hml=("abs_HML", "median"),
            median_abs_umd=("abs_UMD", "median"),
        )
        .reset_index()
    )
    summary.to_csv(TABLE_DIR / "experiment1_residual_purity_summary.csv", index=False)
    summary_by_period.to_csv(TABLE_DIR / "experiment1_residual_purity_by_period.csv", index=False)
    stats_df.to_csv(TABLE_DIR / "experiment1_residual_purity_detail.csv", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    plot_r2 = summary_by_period.pivot(index="period", columns="method", values="median_r2")
    plot_l1 = summary_by_period.pivot(index="period", columns="method", values="median_coef_l1")

    axes[0].plot(plot_r2.index, plot_r2["static"], marker="o", label="static")
    axes[0].plot(plot_r2.index, plot_r2["rolling"], marker="s", label="rolling")
    axes[0].set_title("Residual Regressions: median R^2")
    axes[0].set_ylabel("R^2")
    axes[0].tick_params(axis="x", rotation=20)
    axes[0].grid(alpha=0.25)
    axes[0].legend()

    axes[1].plot(plot_l1.index, plot_l1["static"], marker="o", label="static")
    axes[1].plot(plot_l1.index, plot_l1["rolling"], marker="s", label="rolling")
    axes[1].set_title("Residual Regressions: median |betas| sum")
    axes[1].set_ylabel("L1 norm")
    axes[1].tick_params(axis="x", rotation=20)
    axes[1].grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "experiment1_residual_purity.png", dpi=180)
    plt.close(fig)
    return summary, summary_by_period


def load_representative_stocks(conn, universe_codes):
    latest_date = conn.execute("SELECT MAX(trade_date) FROM daily_basic").fetchone()[0]
    query = f"""
    WITH latest_mv AS (
        SELECT ts_code, total_mv
        FROM daily_basic
        WHERE trade_date = '{latest_date}'
    )
    SELECT l.ts_code, COALESCE(b.name, l.ts_code) AS name, l.total_mv
    FROM latest_mv l
    LEFT JOIN stock_basic b ON l.ts_code = b.ts_code
    """
    try:
        latest = conn.execute(query).fetchdf()
    except duckdb.CatalogException:
        latest = conn.execute(
            f"SELECT ts_code, ts_code AS name, total_mv FROM daily_basic WHERE trade_date = '{latest_date}'"
        ).fetchdf()
    latest = latest[latest["ts_code"].isin(universe_codes)].dropna(subset=["total_mv"]).sort_values("total_mv")
    if latest.empty:
        raise RuntimeError("No latest market-value snapshot available for experiment 2.")
    picks = []
    quantiles = [0.05, 0.35, 0.65, 0.95]
    for q in quantiles:
        row = latest.iloc[min(int(round((len(latest) - 1) * q)), len(latest) - 1)]
        picks.append({"ts_code": row["ts_code"], "name": row["name"], "mv_quantile": q, "total_mv": row["total_mv"]})
    return pd.DataFrame(picks).drop_duplicates(subset=["ts_code"]).reset_index(drop=True)


def rolling_beta_path(stock_df):
    X = stock_df[FACTOR_NAMES].to_numpy(dtype=np.float64, copy=False)
    y = stock_df["pct_chg"].to_numpy(dtype=np.float64, copy=False)
    rows = []
    for end_idx in range(RESID_REG_WINDOW - 1, len(stock_df)):
        start_idx = end_idx - RESID_REG_WINDOW + 1
        beta, _, _, _ = ols_with_stats(X[start_idx : end_idx + 1], y[start_idx : end_idx + 1])
        rows.append(
            {
                "trade_date": int(stock_df.iloc[end_idx]["trade_date"]),
                "intercept": beta[0],
                "MKT_beta": beta[1],
                "SMB_beta": beta[2],
                "HML_beta": beta[3],
                "UMD_beta": beta[4],
            }
        )
    return pd.DataFrame(rows)


def experiment_beta_drift(conn, base_df):
    universe_codes = set(base_df["ts_code"].astype(str).unique())
    picks = load_representative_stocks(conn, universe_codes)
    beta_summaries = []

    for _, pick in picks.iterrows():
        stock_df = (
            base_df[base_df["ts_code"].astype(str) == pick["ts_code"]]
            .sort_values("trade_date")
            .reset_index(drop=True)
        )
        beta_df = rolling_beta_path(stock_df)
        beta_df["date"] = pd.to_datetime(beta_df["trade_date"].astype(str))

        fig, axes = plt.subplots(2, 2, figsize=(12, 7), sharex=True)
        for ax, factor in zip(axes.ravel(), FACTOR_NAMES):
            col = f"{factor}_beta"
            ax.plot(beta_df["date"], beta_df[col], linewidth=1.2)
            ax.axhline(beta_df[col].mean(), color="orange", linestyle="--", linewidth=1)
            ax.set_title(f"{pick['name']} {factor} beta")
            ax.grid(alpha=0.25)
            beta_summaries.append(
                {
                    "ts_code": pick["ts_code"],
                    "name": pick["name"],
                    "factor": factor,
                    "mean_beta": beta_df[col].mean(),
                    "std_beta": beta_df[col].std(),
                    "min_beta": beta_df[col].min(),
                    "max_beta": beta_df[col].max(),
                    "range_beta": beta_df[col].max() - beta_df[col].min(),
                    "sign_changes": int(np.sum(np.sign(beta_df[col].fillna(0)).diff().fillna(0) != 0)),
                }
            )
        fig.suptitle(f"Rolling betas ({pick['name']} / {pick['ts_code']})", y=0.99)
        fig.tight_layout()
        fig.savefig(FIG_DIR / f"experiment2_beta_path_{pick['ts_code']}.png", dpi=180)
        plt.close(fig)

    picks.to_csv(TABLE_DIR / "experiment2_representative_stocks.csv", index=False)
    beta_summary_df = pd.DataFrame(beta_summaries)
    beta_summary_df.to_csv(TABLE_DIR / "experiment2_beta_drift_summary.csv", index=False)
    return picks, beta_summary_df


def load_sample_sets():
    sample_sets = {}
    for sample, file_path in SAMPLE_FILES.items():
        if file_path is None:
            sample_sets[sample] = None
        else:
            sample_sets[sample] = set(pd.read_csv(file_path)["con_code"].astype(str))
    return sample_sets


def prepare_eval_frame(base_df, factor_df, factor_col, sample_codes):
    ret_df = base_df[["ts_code", "trade_date", "pct_chg"]].copy()
    ret_df["ret"] = ret_df["pct_chg"] / 100.0
    ret_df.sort_values(["ts_code", "trade_date"], inplace=True)
    ret_df["next_ret"] = ret_df.groupby("ts_code", observed=True)["ret"].shift(-1)
    merged = factor_df.merge(ret_df[["ts_code", "trade_date", "next_ret"]], on=["ts_code", "trade_date"], how="inner")
    merged = merged.dropna(subset=[factor_col, "next_ret"]).copy()
    if sample_codes is not None:
        merged = merged[merged["ts_code"].astype(str).isin(sample_codes)].copy()
    return merged


def evaluate_period(df, factor_col):
    if df.empty:
        return {
            "obs": 0,
            "daily_points": 0,
            "ic_mean": np.nan,
            "ic_ir": np.nan,
            "spread_ann_ret": np.nan,
            "spread_sharpe": np.nan,
        }

    ic_list = []
    spread_list = []
    valid_days = 0

    for _, day_df in df.groupby("trade_date", observed=True, sort=True):
        if len(day_df) < 30:
            continue
        fac_rank = day_df[factor_col].rank(pct=True, method="first")
        ret_rank = day_df["next_ret"].rank(pct=True, method="first")
        ic = fac_rank.corr(ret_rank, method="pearson")
        if pd.notna(ic):
            ic_list.append(ic)
        groups = np.ceil(fac_rank * 10).clip(1, 10).astype(int)
        grouped = day_df.assign(group=groups).groupby("group", observed=True)["next_ret"].mean()
        if 1 in grouped.index and 10 in grouped.index:
            spread_list.append(grouped.loc[1] - grouped.loc[10])
        valid_days += 1

    ic_series = pd.Series(ic_list, dtype=float)
    spread_series = pd.Series(spread_list, dtype=float)
    ic_mean = ic_series.mean()
    ic_ir = np.nan if ic_series.std(ddof=1) in (0, np.nan) else ic_mean / ic_series.std(ddof=1)
    if len(spread_series) > 0:
        spread_ann = (1 + spread_series).prod() ** (242 / len(spread_series)) - 1
        spread_sharpe = np.nan if spread_series.std(ddof=1) == 0 else spread_series.mean() / spread_series.std(ddof=1) * np.sqrt(242)
    else:
        spread_ann = np.nan
        spread_sharpe = np.nan

    return {
        "obs": len(df),
        "daily_points": valid_days,
        "ic_mean": ic_mean,
        "ic_ir": ic_ir,
        "spread_ann_ret": spread_ann,
        "spread_sharpe": spread_sharpe,
    }


def experiment_stability(base_df, static_spec, rolling_spec):
    sample_sets = load_sample_sets()
    results = []
    common_cols = ["ts_code", "trade_date"]

    for sample_name, codes in sample_sets.items():
        static_eval = prepare_eval_frame(base_df, static_spec[common_cols + ["factor_static"]], "factor_static", codes)
        rolling_eval = prepare_eval_frame(base_df, rolling_spec[common_cols + ["factor_rolling"]], "factor_rolling", codes)

        for period_name, start_d, end_d in PERIODS:
            static_period = static_eval[(static_eval["trade_date"] >= start_d) & (static_eval["trade_date"] <= end_d)]
            rolling_period = rolling_eval[(rolling_eval["trade_date"] >= start_d) & (rolling_eval["trade_date"] <= end_d)]
            for method, frame, factor_col in [
                ("static", static_period, "factor_static"),
                ("rolling", rolling_period, "factor_rolling"),
            ]:
                metrics = evaluate_period(frame, factor_col)
                metrics.update({"sample": sample_name, "period": period_name, "method": method})
                results.append(metrics)

    results_df = pd.DataFrame(results)
    results_df.to_csv(TABLE_DIR / "experiment3_stability_by_period.csv", index=False)

    stability = (
        results_df.groupby(["sample", "method"])
        .agg(
            mean_ic=("ic_mean", "mean"),
            std_ic=("ic_mean", "std"),
            mean_ir=("ic_ir", "mean"),
            std_ir=("ic_ir", "std"),
            mean_spread_ann=("spread_ann_ret", "mean"),
            std_spread_ann=("spread_ann_ret", "std"),
            mean_spread_sharpe=("spread_sharpe", "mean"),
            std_spread_sharpe=("spread_sharpe", "std"),
        )
        .reset_index()
    )
    stability.to_csv(TABLE_DIR / "experiment3_stability_summary.csv", index=False)

    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5), sharey=False)
    for ax, sample_name in zip(axes, SAMPLE_FILES.keys()):
        sub = results_df[results_df["sample"] == sample_name]
        for method, marker in [("static", "o"), ("rolling", "s")]:
            line = sub[sub["method"] == method]
            ax.plot(line["period"], line["ic_mean"], marker=marker, label=method)
        ax.set_title(sample_name)
        ax.set_ylabel("Mean daily IC")
        ax.grid(alpha=0.25)
        ax.tick_params(axis="x", rotation=20)
    axes[0].legend()
    fig.tight_layout()
    fig.savefig(FIG_DIR / "experiment3_period_ic.png", dpi=180)
    plt.close(fig)
    return results_df, stability


def experiment_factor_shift(static_spec, rolling_spec):
    aligned = (
        static_spec.merge(rolling_spec, on=["ts_code", "trade_date"], how="inner")
        .sort_values(["trade_date", "ts_code"])
        .reset_index(drop=True)
    )
    pearson = aligned["factor_static"].corr(aligned["factor_rolling"], method="pearson")
    spearman = aligned["factor_static"].corr(aligned["factor_rolling"], method="spearman")

    daily_corr = (
        aligned.groupby("trade_date", observed=True)
        .apply(
            lambda x: pd.Series(
                {
                    "pearson": x["factor_static"].corr(x["factor_rolling"], method="pearson"),
                    "spearman": x["factor_static"].corr(x["factor_rolling"], method="spearman"),
                }
            )
        )
        .reset_index()
    )
    daily_corr.to_csv(TABLE_DIR / "factor_shift_daily_correlation.csv", index=False)

    summary = pd.DataFrame(
        [
            {
                "aligned_rows": len(aligned),
                "pearson": pearson,
                "spearman": spearman,
                "daily_pearson_mean": daily_corr["pearson"].mean(),
                "daily_spearman_mean": daily_corr["spearman"].mean(),
            }
        ]
    )
    summary.to_csv(TABLE_DIR / "factor_shift_summary.csv", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    sample = aligned.sample(n=min(80000, len(aligned)), random_state=42)
    axes[0].scatter(sample["factor_static"], sample["factor_rolling"], s=3, alpha=0.12)
    axes[0].set_xlabel("Static spec_vol")
    axes[0].set_ylabel("Rolling spec_vol")
    axes[0].set_title("Static vs rolling factor values")
    axes[0].grid(alpha=0.2)
    axes[1].plot(pd.to_datetime(daily_corr["trade_date"].astype(str)), daily_corr["spearman"], linewidth=1)
    axes[1].set_title("Daily cross-sectional Spearman")
    axes[1].grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "factor_shift_comparison.png", dpi=180)
    plt.close(fig)
    return summary


def write_report(exp1_summary, exp1_by_period, picks, beta_summary, period_results, period_stability, factor_shift):
    exp1_pivot = exp1_summary.set_index("method")
    better_r2 = exp1_pivot.loc["rolling", "median_r2"] < exp1_pivot.loc["static", "median_r2"]
    better_l1 = exp1_pivot.loc["rolling", "median_coef_l1"] < exp1_pivot.loc["static", "median_coef_l1"]

    beta_agg = (
        beta_summary.groupby("factor")
        .agg(mean_std_beta=("std_beta", "mean"), mean_range_beta=("range_beta", "mean"))
        .reset_index()
    )

    stability_comp = (
        period_stability.pivot(index="sample", columns="method", values=["std_ic", "std_spread_sharpe"])
        .round(4)
    )

    lines = []
    lines.append("# Rolling-Window Proof Report\n")
    lines.append("## Setup\n")
    lines.append(f"- Residual regression window: `{RESID_REG_WINDOW}` trading days")
    lines.append(f"- Specific-volatility window: `{SPEC_VOL_WINDOW}` trading days")
    lines.append("- Static benchmark: one full-sample beta per stock, then residual standard deviation")
    lines.append("- Rolling method: daily rolling beta, then residual standard deviation\n")

    lines.append("## Experiment 1: Residual purity\n")
    lines.append("Goal: test whether residuals still retain exposure to the four pricing factors.")
    lines.append("")
    lines.append(frame_text(exp1_summary))
    lines.append("")
    lines.append("Subperiod view:")
    lines.append(frame_text(exp1_by_period))
    lines.append("")
    lines.append(
        f"- Rolling residuals have {'lower' if better_r2 else 'higher'} median `R^2` than static residuals."
    )
    lines.append(
        f"- Rolling residuals have {'lower' if better_l1 else 'higher'} median factor-loading L1 norm than static residuals."
    )
    lines.append("- If both are lower, that is direct evidence that rolling windows strip factor exposure more cleanly.\n")

    lines.append("## Experiment 2: Beta drift over time\n")
    lines.append("Goal: verify that stock betas are not constant through time.")
    lines.append("")
    lines.append("Representative stocks:")
    lines.append(frame_text(picks))
    lines.append("")
    lines.append("Rolling-beta summary:")
    lines.append(frame_text(beta_agg))
    lines.append("")
    lines.append("- Large within-stock beta standard deviation or range means a fixed full-sample beta is misspecified.")
    lines.append("- See the per-stock figures in `proof/output/figures/experiment2_beta_path_*.png`.\n")

    lines.append("## Experiment 3: Subperiod stability\n")
    lines.append("Goal: compare static and rolling `spec_vol` across market regimes.")
    lines.append("")
    lines.append(frame_text(period_results))
    lines.append("")
    lines.append("Stability summary (lower standard deviation across periods is better):")
    lines.append(frame_text(period_stability))
    lines.append("")
    lines.append("Selected stability pivot:")
    lines.append(frame_text(stability_comp, index=True))
    lines.append("")

    lines.append("## Factor-value shift\n")
    lines.append("This is not the core proof, but it shows whether rolling materially changes the factor values themselves.")
    lines.append("")
    lines.append(frame_text(factor_shift))
    lines.append("")
    lines.append("- High correlation means the signal direction is broadly preserved.")
    lines.append("- Lower-than-1 correlation means rolling windows are materially re-ordering cross-sectional stock rankings.\n")

    lines.append("## Reading guide\n")
    lines.append("- Best direct proof: Experiment 1. If rolling residuals have lower residual-on-factor `R^2` and lower residual factor-loadings, then rolling windows are more correct by construction.")
    lines.append("- Best intuitive proof: Experiment 2. If betas drift a lot, using one full-sample beta is internally inconsistent.")
    lines.append("- Best practical proof: Experiment 3. If rolling windows keep signal quality more stable across subperiods, they are more robust for research and live use.")
    lines.append("")

    report_path = OUTPUT_DIR / "analysis_report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def main():
    ensure_dirs()
    conn = duckdb.connect(str(DB_PATH), read_only=True)

    print("Loading data from DuckDB...")
    base_df = load_base_data(conn)
    rolling_resids = load_rolling_resids(conn)
    rolling_spec = load_rolling_spec(conn)

    print("Building static benchmark residuals...")
    static_resids = compute_static_residuals(base_df)

    print("Building static benchmark spec_vol...")
    static_spec = compute_static_spec(static_resids)

    print("Running experiment 1...")
    exp1_summary, exp1_by_period = experiment_residual_purity(base_df, static_resids, rolling_resids)

    print("Running experiment 2...")
    picks, beta_summary = experiment_beta_drift(conn, base_df)

    print("Running experiment 3...")
    period_results, period_stability = experiment_stability(base_df, static_spec, rolling_spec)

    print("Comparing factor-value shift...")
    factor_shift = experiment_factor_shift(static_spec, rolling_spec)

    print("Writing report...")
    report_path = write_report(
        exp1_summary=exp1_summary,
        exp1_by_period=exp1_by_period,
        picks=picks,
        beta_summary=beta_summary,
        period_results=period_results,
        period_stability=period_stability,
        factor_shift=factor_shift,
    )
    print(f"Done. Report saved to {report_path}")


if __name__ == "__main__":
    main()
