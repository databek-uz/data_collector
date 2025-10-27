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

# Crow Website

```bash
python .\\workspace\\common\\crawl_site.py `
  --sitemap https://kun.uz/sitemap.xml `
  --out-root data `
  --max-pages 100
```

# Upload dataset

```bash
python .\\workspace\\common\\upload_to_hf.py
```
