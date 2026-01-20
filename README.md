# fe-dispatch

## Introduction

This project is a modified implementation of the StarRocks FE (Frontend). It contains only the modified FE code.

## Usage

### Prerequisites

- StarRocks 4.0.1 source code

#### Install Dependencies

Run the following command to install dependencies:

```bash
sudo apt-get update
sudo apt-get install automake bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 bzip2 -y
```

#### Install Compilers

**For Ubuntu 22.04 and above:**

```bash
sudo apt-get install cmake gcc g++ default-jdk -y
```

**For Ubuntu versions below 22.04:**

Check the versions of tools and compilers:

1. **Check GCC/G++ version:**

   ```bash
   gcc --version
   g++ --version
   ```

   GCC/G++ version must be 10.3 or above. If your version is lower, please install a newer version.

2. **Check JDK version:**

   ```bash
   java --version
   ```

   OpenJDK version must be 8 or above. If your version is lower, please install a newer version.

3. **Check CMake version:**

   ```bash
   cmake --version
   ```

   CMake version must be 3.20.1 or above. If your version is lower, please install a newer version.

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

## Smart Dispatch Implementation

The Smart Dispatch feature is implemented in the following files:

### Core Files

All implementation files are located in `fe-core/src/main/java/com/starrocks/qe/scheduler/dispatch/`:

| File | Description |
|------|-------------|
| `DispatchPolicy.java` | Interface defining the dispatch policy contract |
| `DispatchRequest.java` | Encapsulates dispatch request with fragment contexts and backend load statistics |
| `DispatchResult.java` | Contains the dispatch result mapping fragments to backends |
| `FragmentDispatcher.java` | Main dispatcher that coordinates the dispatch process |
| `GreedyDispatchPolicy.java` | Greedy algorithm implementation for fragment dispatch with load balancing |
| `CostEstimator.java` | Estimates fragment execution cost (CPU, memory, time) using ML models |
| `PythonModelPredictor.java` | Python-based ML model predictor for cost estimation |
| `FragmentCost.java` | Data class representing predicted fragment cost (CPU, memory, time) |
| `FragmentDispatchContext.java` | Context information for fragment dispatch including operator types, scan rows, etc. |
| `BackendLoadStat.java` | Backend load statistics (CPU usage, memory usage, running fragments) |
| `FragmentInstanceStats.java` | Runtime statistics for fragment instances |
| `DispatchTrainingRecorder.java` | Records dispatch data for ML model training |

## Notes

- This project only modifies the FE code; BE (Backend) and other components remain unchanged from the official StarRocks implementation
- Please ensure you use StarRocks version 4.0.1 source code for replacement
