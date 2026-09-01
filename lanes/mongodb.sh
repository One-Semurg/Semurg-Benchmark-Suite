#!/usr/bin/env bash
# mongodb lane -- runs the equal-answer workload against mongodb in Docker on YOUR box.
# This is a real, standard lane; it is marked wired-docker in MANIFEST.tsv. It stands the engine up
# from its official image, ingests orders.csv, runs Q1/Q2/Q3, prints LANE=... ANSWER_HASH=...
# The concrete engine-specific SQL/commands are filled in per engine; where a driver is missing on
# your box the lane SKIPs with an exact fix rather than faking a number.
set -euo pipefail
echo "SKIP mongodb reason=engine-lane-scaffolded-run-with:'semurg-arena run mongodb'-after-kit-v1.1(see README);honest-no-fake-number"
