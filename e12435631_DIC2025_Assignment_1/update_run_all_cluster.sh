#!/bin/bash

# Config - automatically update the .sh in the cluster
STUDENT_ID="e12435631"
CLUSTER_HOST="${STUDENT_ID}@lbd.tuwien.ac.at"
PROJECT_FOLDER="e12435631_DIC2025_Assignment_1"
TARGET_PATH="${PROJECT_FOLDER}/src/run_all_cluster.sh"

# Upload only the run_all_cluster.sh
echo "Uploading updated run_all_cluster.sh to cluster..."

scp "src/run_all_cluster.sh" "${CLUSTER_HOST}:~/${TARGET_PATH}"

echo "end"
