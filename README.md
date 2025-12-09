# MSTR Robotics

A Python toolkit for MicroStrategy automation, object comparison, and Redis-based BI analysis.

## Features

- **Object Comparison**: Compare MicroStrategy objects between environments using JSON diff analysis
- **Redis Integration**: Store and analyze BI metadata using Redis
- **Streamlit UI**: Interactive web interface for object comparison and navigation
- **Migration Tools**: Automate MicroStrategy object migration workflows

## Project Structure

- `mstr_robotics/` - Main package with core functionality
- `notebooks/` - Jupyter notebooks for analysis and exploration
- `Dansfiles/` - Migration and automation scripts
- `config/` - Configuration files (YAML, JSON)
- `export/` - Export utilities and logs
- `alt/` - Alternative implementations and experiments

## Setup

1. Clone the repository
2. Create virtual environment: `python -m venv .venv`
3. Activate: `.venv\Scripts\Activate.ps1`
4. Install dependencies: `pip install -r requirements.txt` (if available)

## Usage

Run the Streamlit comparison interface:
```powershell
streamlit run mstr_robotics/streamLit.py
```

## Development

- Python 3.x required
- Uses Redis for data storage
- Streamlit for web UI
- Jupyter notebooks for exploratory analysis
