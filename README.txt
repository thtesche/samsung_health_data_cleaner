# Samsung Health Dashboard

This project provides a dashboard for analyzing Samsung Health data using Streamlit.

## Prerequisites

- Python 3.8 or higher

## Setup Instructions

1.  **Navigate to the project directory:**

    Open your terminal and change to the project directory:
    ```bash
    cd your_path_to_project/samsung_health_dashboard
    ```

2.  **Create a virtual environment:**

    It is recommended to use a virtual environment to manage dependencies.

    ```bash
    python3 -m venv .venv
    ```

3.  **Activate the virtual environment:**

    *   **macOS/Linux:**
        ```bash
        source .venv/bin/activate
        ```
    *   **Windows:**
        ```bash
        .venv\Scripts\activate
        ```

4.  **Install dependencies:**

    Upgrade pip and install the required libraries:

    ```bash
    pip install --upgrade pip
    pip install streamlit pandas plotly numpy
    ```

## Running the Application

To start the dashboard, run the following command:

```bash
streamlit run health_dashboard.py
```

The application should open automatically in your default web browser.
