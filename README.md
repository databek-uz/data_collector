## Installation

Create and activate a virtual environment (recommended):

```bash
python -m venv venv
# On Linux/Mac
source venv/bin/activate
# On Windows (PowerShell)
.\venv\Scripts\Activate
```

Install JupyterLab and project dependencies:

```bash
pip install -r requirements.txt
```

## Environment Setup

Copy the example environment file and adjust values as needed:

```bash
cp .env.example .env
```

## Usage

Start JupyterLab:

```bash
python -m jupyter lab
```

Select the correct kernel (`Python (venv)` if you created one) and run the notebooks.
