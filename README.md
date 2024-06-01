# RegSnap Plus

Local Recovery and Partial Snapshot based on Apache Flink.

This repository was developed from https://github.com/apache/flink/tree/release-1.14 and modified to realize our research objective.

The in-flight logs implementations are adopted and adjusted from https://github.com/delftdata/Clonos

The previous modification was published at https://github.com/takdir-rex/regsnap

# Features
* Defines a custom snapshot group/region of adjacent pipelined operators 
* Triggers savepoint for the defined regions instead of global snapshots
* Calculate the smallest possible recovery units to recover, instead of global recovery, while maintaining processing integrity with exactly-once guarantee
* Restore the states of the recovered operators from the latest regional snapshots that contain the recovered states, including the global snapshots.

# Evaluation
This artifact was evaluated using:
* Synthetic streaming job. Available at: https://github.com/takdir-rex/synthetic-benchmark
* Windowed Nexmark queries. Available at: https://github.com/takdir-rex/nexmark-windowed
* Automation scripts are available at: https://github.com/takdir-rex/regsnap-plus/tree/main/scripts
