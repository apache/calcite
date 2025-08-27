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
BUILD_DIR="$SCRIPT_DIR/build/distribution"
DIST_NAME="calcite-file-sql"
VERSION="1.0.0"
DIST_DIR="$BUILD_DIR/$DIST_NAME-$VERSION"

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_info() {
    echo -e "${YELLOW}$1${NC}"
}

print_success() {
    echo -e "${GREEN}$1${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
}

# Clean previous build
print_info "Cleaning previous build..."
rm -rf "$BUILD_DIR"
mkdir -p "$DIST_DIR"/{lib,config,examples,bin}

# Build the file adapter
print_info "Building Calcite File Adapter..."
cd "$SCRIPT_DIR/.."
./gradlew :file:build -x test
if [ $? -ne 0 ]; then
    print_error "Failed to build file adapter"
    exit 1
fi

# Create fat JAR with all dependencies
print_info "Creating fat JAR with dependencies..."
./gradlew :file:shadowJar
if [ $? -ne 0 ]; then
    # If shadowJar task doesn't exist, create a custom task
    cat >> file/build.gradle.kts << 'EOF'

// Shadow plugin for creating fat JAR
plugins {
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

tasks.shadowJar {
    archiveBaseName.set("calcite-file-adapter")
    archiveClassifier.set("all")
    manifest {
        attributes["Main-Class"] = "sqlline.SqlLine"
    }
}
EOF
    ./gradlew :file:shadowJar
fi

# Copy JAR files
print_info "Copying JAR files..."
if [ -f file/build/libs/*-all.jar ]; then
    cp file/build/libs/*-all.jar "$DIST_DIR/lib/calcite-file-adapter.jar"
else
    # Fallback: copy all dependencies individually
    ./gradlew :file:copyDependencies
    cp file/build/libs/*.jar "$DIST_DIR/lib/"
    cp file/build/dependencies/*.jar "$DIST_DIR/lib/" 2>/dev/null
fi

# Copy the main script
print_info "Copying scripts..."
cp "$SCRIPT_DIR/sqlline-file-adapter.sh" "$DIST_DIR/bin/calcite-file-sql"
chmod +x "$DIST_DIR/bin/calcite-file-sql"

# Create a simplified launcher script
cat > "$DIST_DIR/calcite-file-sql" << 'EOF'
#!/bin/bash
# Launcher script for Calcite File SQL
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/bin/calcite-file-sql" "$@"
EOF
chmod +x "$DIST_DIR/calcite-file-sql"

# Create example models
print_info "Creating example models..."

# CSV example
cat > "$DIST_DIR/examples/csv-model.json" << 'EOF'
{
  "version": "1.0",
  "defaultSchema": "CSV",
  "description": "Example CSV file adapter model",
  "schemas": [
    {
      "name": "CSV",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "data",
        "flavor": "scannable"
      }
    }
  ]
}
EOF

# Parquet example
cat > "$DIST_DIR/examples/parquet-model.json" << 'EOF'
{
  "version": "1.0",
  "defaultSchema": "PARQUET",
  "description": "Example Parquet file adapter model",
  "schemas": [
    {
      "name": "PARQUET",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "data",
        "parquet": true,
        "executionEngine": "duckdb"
      }
    }
  ]
}
EOF

# JSON example
cat > "$DIST_DIR/examples/json-model.json" << 'EOF'
{
  "version": "1.0",
  "defaultSchema": "JSON",
  "description": "Example JSON file adapter model",
  "schemas": [
    {
      "name": "JSON",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "data"
      }
    }
  ]
}
EOF

# Multi-format example
cat > "$DIST_DIR/examples/multi-format-model.json" << 'EOF'
{
  "version": "1.0",
  "defaultSchema": "DATA",
  "description": "Example multi-format file adapter model",
  "schemas": [
    {
      "name": "DATA",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": ".",
        "recursive": true,
        "glob": "**/*.{csv,json,parquet}",
        "executionEngine": "auto"
      }
    }
  ]
}
EOF

# Create sample data directory
mkdir -p "$DIST_DIR/data"

# Create sample CSV file
cat > "$DIST_DIR/data/employees.csv" << 'EOF'
id,name,department,salary
1,John Doe,Engineering,75000
2,Jane Smith,Marketing,65000
3,Bob Johnson,Sales,70000
4,Alice Brown,Engineering,80000
5,Charlie Wilson,HR,60000
EOF

# Create sample JSON file
cat > "$DIST_DIR/data/products.json" << 'EOF'
[
  {"id": 1, "name": "Laptop", "category": "Electronics", "price": 999.99},
  {"id": 2, "name": "Mouse", "category": "Electronics", "price": 29.99},
  {"id": 3, "name": "Desk", "category": "Furniture", "price": 299.99},
  {"id": 4, "name": "Chair", "category": "Furniture", "price": 199.99},
  {"id": 5, "name": "Monitor", "category": "Electronics", "price": 399.99}
]
EOF

# Create default config
cat > "$DIST_DIR/config/default.properties" << 'EOF'
# Calcite File Adapter Configuration
calcite.parquet.cache.enabled=true
calcite.parquet.cache.size=100
calcite.execution.engine=auto
calcite.file.recursive=false
EOF

# Create README
cat > "$DIST_DIR/README.md" << 'EOF'
# Calcite File SQL Shell

A standalone SQL interface for querying files using Apache Calcite File Adapter.

## Quick Start

1. Run with default model:
   ```bash
   ./calcite-file-sql
   ```

2. Run with example model:
   ```bash
   ./calcite-file-sql examples/csv-model.json
   ```

3. Execute a query:
   ```bash
   ./calcite-file-sql -c "SELECT * FROM employees"
   ```

## Supported Formats

- CSV
- JSON
- Parquet
- Arrow
- Excel (XLS/XLSX)
- XML
- YAML
- Compressed files (GZ, BZ2, XZ)

## Directory Structure

```
calcite-file-sql/
├── bin/                 # Executable scripts
├── lib/                 # JAR dependencies
├── config/              # Configuration files
├── examples/            # Example model files
├── data/                # Sample data files
└── README.md
```

## Creating a Model File

Create a `model.json` file:

```json
{
  "version": "1.0",
  "defaultSchema": "MY_DATA",
  "schemas": [
    {
      "name": "MY_DATA",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/path/to/data"
      }
    }
  ]
}
```

## Commands

Inside the SQL shell:
- `!help` - Show help
- `!tables` - List tables
- `!describe <table>` - Show table schema
- `!quit` - Exit

## License

Apache License 2.0
EOF

# Create version file
echo "$VERSION" > "$DIST_DIR/VERSION"

# Create archive
print_info "Creating distribution archive..."
cd "$BUILD_DIR"
tar -czf "$DIST_NAME-$VERSION.tar.gz" "$DIST_NAME-$VERSION"
zip -qr "$DIST_NAME-$VERSION.zip" "$DIST_NAME-$VERSION"

print_success "Distribution created successfully!"
echo ""
echo "Distribution packages:"
echo "  - $BUILD_DIR/$DIST_NAME-$VERSION.tar.gz"
echo "  - $BUILD_DIR/$DIST_NAME-$VERSION.zip"
echo ""
echo "To test the distribution:"
echo "  cd $DIST_DIR"
echo "  ./calcite-file-sql examples/csv-model.json"
