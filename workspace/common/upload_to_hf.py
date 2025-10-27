import os
from pathlib import Path
from dotenv import load_dotenv
from huggingface_hub import create_repo, upload_file

REPO_ID   = "databek/uzbek_news_site"
DATA_DIR  = Path("data")

def main():
    load_dotenv()
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise RuntimeError("HF_TOKEN topilmadi (.env faylga qo'ying)")

    # repo mavjud bo'lmasa yaratamiz
    create_repo(repo_id=REPO_ID, repo_type="dataset", exist_ok=True, token=hf_token)

    # data/*.parquet ni topamiz
    files = sorted(DATA_DIR.glob("*.parquet"))
    if not files:
        print("⚠️  data/*.parquet topilmadi.")
        return

    uploaded = 0
    for f in files:
        print(f"⬆️  Upload: {f}  ->  {REPO_ID}:{f.name}")
        upload_file(
            path_or_fileobj=str(f),
            path_in_repo=f"data/{f.name}",
            repo_id=REPO_ID,
            repo_type="dataset",
            token=hf_token,
            commit_message=f"auto: upload {f.name}",
        )
        uploaded += 1

    print(f"✅ {uploaded} ta fayl yuklandi: https://huggingface.co/datasets/{REPO_ID}")

if __name__ == "__main__":
    main()
