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
    --output-dir datasets `
    --max-pages 5000 `
    --max-workers 20 `
    --checkpoint-every 200 `
    --rate-limit 0.05 `
    --log-level INFO
```

# Upload dataset

```bash
python .\\workspace\\common\\upload_to_hf.py
```
