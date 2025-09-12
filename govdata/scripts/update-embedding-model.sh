#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODEL_DIR="$SCRIPT_DIR/../src/main/resources/models"

echo "=========================================="
echo "SEC Embedding Model Update Tool"
echo "=========================================="
echo ""
echo "Model files location: $MODEL_DIR"
echo ""

# Function to add vocabulary term
add_vocab_term() {
    echo "Enter term to add:"
    read term
    echo "Enter weight (1.0-3.0, higher = more important):"
    read weight

    echo "$term|$weight" >> "$MODEL_DIR/financial-vocabulary.txt"
    echo "✓ Added: $term with weight $weight"
}

# Function to show current stats
show_stats() {
    vocab_count=$(grep -v "^#" "$MODEL_DIR/financial-vocabulary.txt" | grep -v "^$" | wc -l)
    echo "Current vocabulary size: $vocab_count terms"

    echo ""
    echo "Top 10 terms by weight:"
    grep -v "^#" "$MODEL_DIR/financial-vocabulary.txt" | grep -v "^$" | \
        sort -t'|' -k2 -rn | head -10
}

# Menu
while true; do
    echo ""
    echo "Options:"
    echo "  1. Show current model statistics"
    echo "  2. Add vocabulary term"
    echo "  3. Edit vocabulary file (opens in default editor)"
    echo "  4. Edit context mappings (opens in default editor)"
    echo "  5. Backup current model"
    echo "  6. Exit"
    echo ""
    echo -n "Select option: "
    read option

    case $option in
        1)
            show_stats
            ;;
        2)
            add_vocab_term
            ;;
        3)
            ${EDITOR:-vi} "$MODEL_DIR/financial-vocabulary.txt"
            ;;
        4)
            ${EDITOR:-vi} "$MODEL_DIR/context-mappings.json"
            ;;
        5)
            backup_dir="$MODEL_DIR/backup-$(date +%Y%m%d-%H%M%S)"
            mkdir -p "$backup_dir"
            cp "$MODEL_DIR"/*.txt "$MODEL_DIR"/*.json "$backup_dir/" 2>/dev/null
            echo "✓ Model backed up to: $backup_dir"
            ;;
        6)
            echo "Exiting..."
            exit 0
            ;;
        *)
            echo "Invalid option"
            ;;
    esac
done
