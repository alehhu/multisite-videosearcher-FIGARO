"""Load the runs of the two-site pipeline, and draw their plots and tables.

A run is a directory written by scripts/summarize_run.py --out, with Locust's files next to
the CSV files and a run.yaml that describes the run:

    date: 2026-11-02
    commit: 116b3c2
    placement: split-ffmpeg2
    agents: old                       # which checkpoints scaled the stages
    load: locust_test.py, 60 min
    duration_s: 3600                  # the --duration given to summarize_run.py
    max_replicas: {ffmpeg0: 10, librosa: 10, ffmpeg1: 10, ffmpeg2: 10, deepspeech: 10, grep: 10}
    thresholds: {ffmpeg0: 2.03, librosa: 6.0, ffmpeg1: 2.08, ffmpeg2: 93.02, deepspeech: 103.52, grep: 0.17}
    notes: first run with the original controller's state

Only duration_s and thresholds are read by the plots, the rest goes in config_table. They
are kept per run because they change between runs: a run is always judged by the thresholds
and the duration it had.

Every plot takes a list of runs (at most three, they get one colour each) and, optionally,
the directory where to save the figure as a PNG.
"""
import os
import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

STAGES = ["ffmpeg0", "librosa", "ffmpeg1", "ffmpeg2", "deepspeech", "grep"]
BIN = 60                # seconds per point of the time series
WINDOW = 120            # seconds per point of the per-stage plots (the agents' window)
GLOBAL_THRESHOLD = 200  # seconds: end-to-end threshold used in the single-site thesis
PER_BIN = "minute" if BIN == 60 else f"{BIN:g} s"

# Fixed colours: sites and runs keep the same colour in every plot.
SITE_COLORS = {"edge": "#2a78d6", "cloud": "#eb6834"}
RUN_PALETTE = ["#2a78d6", "#eb6834", "#1baf7a"]
OBS_COLORS = dict(zip(["workload", "utilization", "pressure", "queue_length_dominant", "n_instances"],
                      ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]))
INK, MUTED = "#0b0b0b", "#8a8984"

plt.rcParams.update({
    "figure.dpi": 110, "figure.figsize": (10, 3.2), "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": "#e4e4e0", "grid.linewidth": 0.6, "axes.axisbelow": True,
    "axes.edgecolor": MUTED, "axes.labelcolor": "#52514e", "xtick.color": "#52514e",
    "ytick.color": "#52514e", "lines.linewidth": 1.5, "legend.frameon": False, "font.size": 9,
})


def controller_thresholds():
    """{stage: res_time_threshold} of deploy/k8s/base/controller-config.yaml."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "deploy", "k8s", "base",
                        "controller-config.yaml")
    with open(path) as f:
        return {stage: cfg["res_time_threshold"] for stage, cfg in yaml.safe_load(f)["stages"].items()}


class Run:
    """The tables of one run, and what its run.yaml says about it."""

    def __init__(self, name, path):
        self.name, self.path = name, path
        with open(os.path.join(path, "run.yaml")) as f:
            self.config = yaml.safe_load(f)
        # run.yaml from scripts/run-experiment.sh has `minutes` and no thresholds: those of the
        # controller configuration apply (the ones the agents were given during the run).
        self.duration = self.config.get("duration_s") or self.config["minutes"] * 60
        self.thresholds = self.config.get("thresholds") or controller_thresholds()
        missing = [s for s in STAGES if s not in self.thresholds]
        if missing:
            raise ValueError(f"{name}: run.yaml has no threshold for {missing}")

        for table in ("jobs", "stages", "replicas", "agents"):
            setattr(self, table, pd.read_csv(os.path.join(path, f"{table}.csv")))
        late = self.jobs[(self.jobs["ended_in_run"] == 1) & (self.jobs["end"] > self.duration + 5)]
        if len(late):
            raise ValueError(f"{name}: {len(late)} jobs counted in the run end after duration_s = "
                             f"{self.duration}: summarize_run.py was given another --duration")

        self.requests = self.profile = None
        requests = os.path.join(path, "request_timestamps.csv")
        if os.path.exists(requests):
            self.requests = pd.read_csv(requests)
            self.requests["t"] = self.requests["Timestamp"] - self.requests["Timestamp"].min()
        profile = os.path.join(path, "workloadProfile.csv")
        if os.path.exists(profile):
            self.profile = pd.read_csv(profile, sep=";")

        s = self.stages
        # Response time of a stage: from the previous stage handing the job over to the end.
        s["response_time"] = s["wait"].fillna(0) + s["service_time"]
        # Total service time of every job: the sum of its stages.
        self.jobs = self.jobs.merge(s.groupby("job_id")["service_time"].sum().rename("service_time"),
                                    on="job_id", how="left")

    def completed(self):
        j = self.jobs
        return j[(j["outcome"] == "completed") & (j["ended_in_run"] == 1)]

    def arrivals_per_bin(self):
        edges = np.arange(0, self.duration + BIN, BIN)
        counts, _ = np.histogram(self.jobs["arrival"], bins=edges)
        return edges[:-1], counts

    def replica_hours(self):
        r = self.replicas.sort_values("t")
        r = r[(r["t"] >= 0) & (r["t"] <= self.duration)].copy()
        r["dt"] = (r.groupby(["site", "stage"])["t"].shift(-1) - r["t"]).fillna(0)
        return (r["alive"] * r["dt"]).groupby([r["stage"], r["site"]]).sum() / 3600

    def violations(self):
        """% of jobs whose response time at a stage exceeds its threshold by more than 10 %."""
        s = self.stages
        limit = s["stage"].map(self.thresholds) * 1.1
        return (s["response_time"] > limit).groupby(s["stage"]).mean() * 100


def load_runs(paths):
    """{name: directory} -> list of Run."""
    return [Run(name, path) for name, path in paths.items()]


def slug(text):
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")


def run_colors(runs):
    if len(runs) > len(RUN_PALETTE):
        raise ValueError(f"a plot compares at most {len(RUN_PALETTE)} runs, got {len(runs)}")
    return {run.name: color for run, color in zip(runs, RUN_PALETTE)}


def finish(fig, save_to, name):
    fig.tight_layout()
    if save_to:
        os.makedirs(save_to, exist_ok=True)
        fig.savefig(os.path.join(save_to, f"{name}.png"), dpi=200, bbox_inches="tight")


# Tables

def config_table(runs):
    """What every run was: one column per run."""
    rows = {}
    for run in runs:
        c = run.config
        rows[run.name] = {
            "date": c.get("date"), "commit": c.get("commit"), "placement": c.get("placement"),
            "agents": c.get("agents"), "load": c.get("load"), "duration (s)": run.duration,
            **{f"max {s}": c.get("max_replicas", {}).get(s) for s in STAGES},
            **{f"threshold {s} (s)": run.thresholds[s] for s in STAGES},
            "notes": c.get("notes"),
        }
    return pd.DataFrame(rows)


def response_table(runs):
    rows = {}
    for run in runs:
        j = run.jobs
        done = run.completed()["duration"]
        rows[run.name] = {
            "completed in the run": len(done),
            "completed after the end": int(((j["outcome"] == "completed") & (j["ended_in_run"] == 0)).sum()),
            "failed": int((j["outcome"] == "failed").sum()),
            **{f"p{q} (s)": done.quantile(q / 100) for q in (50, 90, 95, 99)},
            "mean (s)": done.mean(),
            "max (s)": done.max(),
            f"over {GLOBAL_THRESHOLD} s (%)": (done > GLOBAL_THRESHOLD).mean() * 100,
        }
    return pd.DataFrame(rows).T.round(2)


def service_table(runs):
    """Service time per stage, site and run, with the threshold it had and 5 x the median
    (the rule of the single-site thesis, parse_complete.ipynb)."""
    table = pd.concat({run.name: run.stages for run in runs}, names=["run"]).reset_index(level=0)
    summary = (table.groupby(["stage", "site", "run"])["service_time"]
               .describe(percentiles=[0.05, 0.5, 0.95])
               .rename(columns={"count": "n"}))
    summary = summary.reindex(STAGES, level="stage")
    by_name = {run.name: run for run in runs}
    summary["threshold in use"] = [by_name[r].thresholds[stage] for stage, _, r in summary.index]
    summary["5 x median"] = summary["50%"] * 5
    return summary.round(3)


def replica_hours_table(runs):
    hours = pd.DataFrame({run.name: run.replica_hours() for run in runs})
    return hours.reindex(STAGES, level=0).round(2)


def violations_table(runs):
    return pd.DataFrame({run.name: run.violations() for run in runs}).reindex(STAGES).round(1)


def compare_table(runs):
    """The key numbers of every run, side by side. Any number of runs."""
    rows = {}
    for run in runs:
        done = run.completed()
        rows[run.name] = {
            "duration (s)": run.duration,
            "jobs completed in the run": len(done),
            "failed": int((run.jobs["outcome"] == "failed").sum()),
            "response time p50 (s)": done["duration"].median(),
            "response time p95 (s)": done["duration"].quantile(0.95),
            f"over {GLOBAL_THRESHOLD} s (%)": (done["duration"] > GLOBAL_THRESHOLD).mean() * 100,
            "replica-hours": run.replica_hours().sum(),
            **{f"violations {s} (%)": run.violations().get(s, np.nan) for s in ("ffmpeg2", "deepspeech")},
        }
    return pd.DataFrame(rows).round(2)


# Plots

def plot_load(runs, save_to=None):
    colors = run_colors(runs)
    fig, axes = plt.subplots(len(runs), 1, sharex=True, figsize=(10, 2.6 * len(runs)), squeeze=False)
    for ax, run in zip(axes[:, 0], runs):
        t, counts = run.arrivals_per_bin()
        ax.bar(t / 60, counts, width=BIN / 60 * 0.9, align="edge", color=colors[run.name],
               label=f"arrived (jobs per {PER_BIN})")
        if run.profile is not None:
            p = run.profile
            planned = p.groupby(p["Time"] // BIN)["RPS"].sum()
            ax.step(planned.index * BIN / 60, planned.values, where="post", color=INK,
                    label=f"planned by Locust (jobs per {PER_BIN})")
        ax.set_title(run.name, loc="left")
        ax.set_ylabel(f"jobs per {PER_BIN}")
        ax.legend(loc="upper right")
    axes[-1, 0].set_xlabel("minutes from the start")
    finish(fig, save_to, "load")


def plot_response_time(runs, save_to=None):
    colors = run_colors(runs)
    fig, axes = plt.subplots(len(runs), 1, sharex=True, sharey=True, figsize=(10, 3.4 * len(runs)), squeeze=False)
    for ax, run in zip(axes[:, 0], runs):
        j = run.completed()
        color = colors[run.name]
        ax.scatter(j["arrival"] / 60, j["duration"], s=6, color=color, alpha=0.4, linewidths=0,
                   label="response time (each job)")
        w = j.groupby(j["arrival"] // WINDOW)
        x = w.size().index * WINDOW / 60 + WINDOW / 120
        ax.plot(x, w["duration"].mean().values, color=color, marker="o", markersize=3,
                label=f"response time (mean per {WINDOW:g} s)")
        ax.plot(x, w["service_time"].mean().values, color=INK, marker="x", markersize=4,
                label=f"sum of service times (mean per {WINDOW:g} s)")
        ax.axhline(GLOBAL_THRESHOLD, color=INK, linestyle="--", linewidth=1, label=f"threshold ({GLOBAL_THRESHOLD} s)")
        ax.set_title(run.name, loc="left")
        ax.set_ylabel("seconds")
        ax.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0))
    axes[-1, 0].set_xlabel("arrival (minutes from the start)")
    finish(fig, save_to, "response_time")


def plot_response_cdf(runs, save_to=None):
    colors = run_colors(runs)
    fig, ax = plt.subplots(figsize=(8, 3.6))
    for run in runs:
        d = np.sort(run.completed()["duration"])
        if not len(d):
            continue
        ax.plot(d, np.arange(1, len(d) + 1) / len(d) * 100, color=colors[run.name], label=run.name)
        for q, style in ((50, ":"), (95, "--")):
            ax.axvline(np.percentile(d, q), color=colors[run.name], linestyle=style, linewidth=1)
    ax.set_xlabel("job response time (s)")
    ax.set_ylabel("% of jobs")
    ax.set_title("Response time CDF (dotted: p50, dashed: p95)", loc="left")
    if len(runs) > 1:
        ax.legend()
    finish(fig, save_to, "response_time_cdf")


def plot_stage_times(runs, save_to=None):
    colors = run_colors(runs)
    fig, axes = plt.subplots(2, 3, sharex=True, figsize=(13, 6.4))
    for ax, stage in zip(axes.flat, STAGES):
        for run in runs:
            color = colors[run.name]
            s = run.stages[run.stages["stage"] == stage]
            w = s.groupby(s["end"] // WINDOW)
            x = w.size().index * WINDOW / 60 + WINDOW / 120
            ax.plot(x, w["response_time"].mean().values, color=color, marker="o", markersize=3,
                    label=f"response ({run.name})" if len(runs) > 1 else "response time")
            ax.plot(x, w["service_time"].mean().values, color=color, linestyle="--", linewidth=1,
                    label=f"service ({run.name})" if len(runs) > 1 else "service time")
            # Every run has its own threshold: the line takes the colour of the run.
            ax.axhline(run.thresholds[stage], color=INK if len(runs) == 1 else color, linestyle=":", linewidth=1.2,
                       label=f"threshold ({run.name})" if len(runs) > 1 else "threshold")
        ax.set_title(stage, loc="left")
        ax.set_ylim(bottom=0)
    for ax in axes[:, 0]:
        ax.set_ylabel("seconds")
    for ax in axes[-1]:
        ax.set_xlabel("minutes from the start")
    axes[0, -1].legend(loc="upper left", bbox_to_anchor=(1.0, 1.0))
    finish(fig, save_to, "stages_response_service")


def plot_service_time(runs, save_to=None):
    fig, axes = plt.subplots(2, 3, figsize=(12, 6.4))
    for ax, stage in zip(axes.flat, STAGES):
        data, labels, colors = [], [], []
        for run in runs:
            s = run.stages[run.stages["stage"] == stage]
            for site in SITE_COLORS:
                values = s.loc[s["site"] == site, "service_time"]
                if len(values):
                    data.append(values)
                    labels.append(site if len(runs) == 1 else f"{site}\n{run.name}")
                    colors.append(SITE_COLORS[site])
        if data:
            box = ax.boxplot(data, patch_artist=True, widths=0.6,
                             flierprops={"markersize": 3, "markeredgecolor": MUTED},
                             medianprops={"color": INK})
            for patch, color in zip(box["boxes"], colors):
                patch.set_facecolor(color)
                patch.set_edgecolor(color)
            ax.set_xticks(range(1, len(labels) + 1), labels)
        ax.set_title(stage, loc="left")
    for ax in axes[:, 0]:
        ax.set_ylabel("service time (s)")
    finish(fig, save_to, "service_time")


def plot_service_vs_wait(runs, save_to=None):
    colors = run_colors(runs)
    fig, axes = plt.subplots(2, 3, figsize=(12, 6.4))
    for ax, stage in zip(axes.flat, STAGES):
        for run in runs:
            s = run.stages[run.stages["stage"] == stage]
            ax.scatter(s["service_time"], s["wait"], s=6, alpha=0.4, linewidths=0,
                       color=colors[run.name], label=run.name)
        ax.set_title(stage, loc="left")
    for ax in axes[-1]:
        ax.set_xlabel("service time (s)")
    for ax in axes[:, 0]:
        ax.set_ylabel("wait (s)")
    if len(runs) > 1:
        for handle in axes[0, 0].legend(markerscale=2).legend_handles:
            handle.set_alpha(1)
    finish(fig, save_to, "service_vs_wait")


def plot_waits(runs, save_to=None, stages=STAGES):
    """Mean wait before every stage, per minute of start. `stages` to show only some of them:
    the light stages wait milliseconds, which is noise next to the queues of the heavy ones."""
    colors = run_colors(runs)
    cols = min(3, len(stages))
    rows = -(-len(stages) // cols)
    fig, axes = plt.subplots(rows, cols, sharex=True, figsize=(4 * cols, 2.6 * rows), squeeze=False)
    for ax, stage in zip(axes.flat, stages):
        for run in runs:
            s = run.stages[run.stages["stage"] == stage]
            w = s.groupby(s["start"] // BIN)["wait"].mean()
            ax.plot(w.index * BIN / 60, w.values, color=colors[run.name], label=run.name)
        site = "+".join(sorted(set(run.stages.loc[run.stages["stage"] == stage, "site"].dropna())))
        ax.set_title(f"{stage} ({site})" if site else stage, loc="left")
    for ax in axes.flat[len(stages):]:
        ax.set_visible(False)
    for ax in axes[:, 0]:
        ax.set_ylabel("mean wait (s)")
    for ax in axes[-1]:
        ax.set_xlabel("minutes from the start")
    if len(runs) > 1:
        axes[0, 0].legend()
    finish(fig, save_to, "waits" if list(stages) == STAGES else "waits_" + "_".join(stages))


def plot_replica_queues(runs, stage="deepspeech", save_to=None):
    """Every job of a stage as a dot: when it started (x), on which replica (y), how long it
    waited (colour). The broker hands a job to a replica when the job is published, busy or
    not: a replica started later only gets the jobs published after it, while the queue
    already handed out stays on the older ones. One figure per run."""
    for run in runs:
        s = run.stages[run.stages["stage"] == stage].copy()
        if s.empty:
            continue
        first = s.groupby("pod")["start"].min().sort_values()
        order = {pod: i for i, pod in enumerate(first.index)}
        s["row"] = s["pod"].map(order)
        fig, ax = plt.subplots(figsize=(10, 0.45 * len(order) + 1.4))
        dots = ax.scatter(s["start"] / 60, s["row"], c=s["wait"], cmap="YlOrRd", vmin=0,
                          s=18, edgecolors=INK, linewidths=0.2)
        labels = []
        for pod, i in order.items():
            jobs = s[s["pod"] == pod]
            labels.append(f"replica {i + 1}: {len(jobs)} jobs, mean wait {jobs['wait'].mean():.0f} s")
        ax.set_yticks(range(len(order)), labels)
        ax.invert_yaxis()
        ax.set_xlabel("start of the job on the replica (minutes from the start)")
        ax.grid(axis="y", visible=False)
        fig.colorbar(dots, ax=ax, label="wait (s)", pad=0.01)
        site = "+".join(sorted(set(s["site"].dropna())))
        ax.set_title(f"{run.name}: jobs of {stage} ({site}) by replica, in order of start", loc="left")
        finish(fig, save_to, f"replica_queues_{slug(run.name)}_{stage}")


def plot_replicas(runs, save_to=None):
    """One figure per run."""
    for run in runs:
        fig, axes = plt.subplots(len(STAGES) + 1, 1, sharex=True, figsize=(10, 1.5 * (len(STAGES) + 1)))
        t, counts = run.arrivals_per_bin()
        axes[0].bar(t / 60, counts, width=BIN / 60 * 0.9, align="edge", color=MUTED)
        axes[0].set_ylabel(f"jobs per {PER_BIN}")
        axes[0].set_title(f"{run.name}: load and replicas", loc="left")
        r = run.replicas
        for ax, stage in zip(axes[1:], STAGES):
            for site, color in SITE_COLORS.items():
                s = r[(r["stage"] == stage) & (r["site"] == site)]
                if s["alive"].any() or s["desired"].any():
                    ax.step(s["t"] / 60, s["alive"], where="post", color=color)
                    ax.step(s["t"] / 60, s["desired"], where="post", color=color, linestyle="--", linewidth=1)
            ax.set_ylabel(stage, rotation=0, ha="right", va="center")
            # From 0 and with room on top: a stage that keeps its replicas would get a meaningless scale.
            ax.set_ylim(0, r.loc[r["stage"] == stage, ["alive", "desired"]].max().max() + 1)
            ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
        handles = [plt.Line2D([], [], color=c, label=s) for s, c in SITE_COLORS.items()]
        handles += [plt.Line2D([], [], color=MUTED, label="alive"),
                    plt.Line2D([], [], color=MUTED, linestyle="--", linewidth=1, label="desired")]
        axes[1].legend(handles=handles, loc="upper left", bbox_to_anchor=(1.0, 1.0))
        axes[-1].set_xlabel("minutes from the start")
        axes[-1].set_xlim(0, run.duration / 60)
        finish(fig, save_to, f"replicas_{slug(run.name)}")


def plot_agents(runs, save_to=None):
    """One figure per run and stage."""
    for run in runs:
        decided = run.agents[run.agents["action"].notna()]
        if decided.empty:
            print(f"{run.name}: no agent decisions")
            continue
        for (stage, site), d in decided.groupby(["stage", "site"]):
            fig, (top, bottom) = plt.subplots(2, 1, sharex=True, figsize=(10, 3.6), height_ratios=[3, 2])
            for key, color in OBS_COLORS.items():
                top.plot(d["t"] / 60, d[f"obs_{key}"], color=color, marker="o", markersize=3, label=key)
            top.set_ylim(-0.05, 1.05)
            top.set_ylabel("state (0-1)")
            top.set_title(f"{run.name}: agent of {stage} on {site}", loc="left")
            top.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0))
            bottom.step(d["t"] / 60, d["applied"], where="post", color=SITE_COLORS[site], label="replicas applied")
            bottom.plot(d["t"] / 60, d["action"], "o", color=INK, markersize=4, label="agent's answer")
            bottom.set_ylabel("replicas")
            bottom.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
            bottom.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0))
            bottom.set_xlabel("minutes from the start")
            finish(fig, save_to, f"agent_{slug(run.name)}_{stage}_{site}")


def plot_replica_hours(runs, save_to=None):
    colors = run_colors(runs)
    per_stage = {run.name: run.replica_hours().groupby(level="stage").sum().reindex(STAGES).fillna(0) for run in runs}
    fig, ax = plt.subplots(figsize=(10, 3.2))
    width = 0.8 / len(runs)
    for i, run in enumerate(runs):
        ax.bar(np.arange(len(STAGES)) + i * width, per_stage[run.name], width=width * 0.92,
               color=colors[run.name], label=run.name)
    ax.set_xticks(np.arange(len(STAGES)) + width * (len(runs) - 1) / 2, STAGES)
    ax.set_ylabel("replica-hours")
    ax.set_title("Resources used per stage (both sites)", loc="left")
    if len(runs) > 1:
        ax.legend()
    finish(fig, save_to, "replica_hours")
