import os
from dotenv import load_dotenv
from huggingface_hub import create_repo, upload_folder

load_dotenv()

HF_TOKEN = os.environ.get("HF_TOKEN")
REPO_ID = "databek/uzbek_news_site"

create_repo(
    # create to repo
    repo_id=REPO_ID,
    repo_type="dataset",
    exist_ok=True,
    token=HF_TOKEN,
)

upload_folder(
    # upload to folder
    repo_id=REPO_ID,
    repo_type="dataset",
    folder_path="./data/parquet",
    path_in_repo="data",
    token=HF_TOKEN,
)

print(f"✅ Yuklandi: https://huggingface.co/datasets/{REPO_ID}")
