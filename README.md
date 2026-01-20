# fe-dispatch

## Introduction

This project is a modified implementation of the StarRocks FE (Frontend). It contains only the modified FE code.

## Usage

### Prerequisites

- StarRocks 4.0.1 source code

### Installation Steps

1. **Download StarRocks Source Code**

   ```bash
   git clone https://github.com/StarRocks/starrocks.git
   cd starrocks
   git checkout 4.0.1
   ```

2. **Replace FE Code**

   Replace the `fe` directory in the StarRocks source code with this project:

   ```bash
   rm -rf fe
   cp -r /path/to/this/project fe
   ```

3. **Build and Start**

   Follow the official StarRocks documentation to build and start:

   ```bash
   # Build
   ./build.sh
   
   # Start FE
   ./fe/bin/start_fe.sh --daemon
   ```

   For detailed build and deployment instructions, refer to the [StarRocks Official Documentation](https://docs.starrocks.io/).
