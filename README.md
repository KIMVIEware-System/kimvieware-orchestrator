# KIMVIware Orchestrator

This is the main orchestrator for the KIMVIware system. It provides a web interface to manage and monitor the entire pipeline.

## Prerequisites

*   Python 3.8+
*   The core infrastructure is running (see `kimvieware-infrastructure`).

## Setup

1.  **Install the shared library**:
    The orchestrator depends on the `kimvieware-shared` library. To install it, navigate to the `kimvieware-shared` directory and run:

    ```bash
    pip install -e .
    ```

2.  **Install dependencies**:
    From the `kimvieware-orchestrator` directory, install the required Python packages:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

To start the orchestrator server, run the following command from this directory:

```bash
python run_orchestrator.py
```

The server will be available at `http://localhost:8080`.

## Features

*   **Submit SUT**: Upload and analyze a System Under Test (SUT).
*   **Jobs**: View all jobs in real-time.
*   **Services**: Check the status of the microservices.
*   **Statistics**: View complete statistics.
*   **WebSocket**: Real-time updates at `ws://localhost:8080/ws`.
*   **API**: The API is available at `http://localhost:8080/api/`.
