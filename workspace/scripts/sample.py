import os
import pandas as pd

df = pd.DataFrame(
    [
        {
            "id": "1",
            "text": "O‘zbekiston AI bo‘yicha tashabbus boshladi.",
            "source": "demo",
            "lang": "uz",
        },
        {
            "id": "2",
            "text": "Databek korpusi uchun birinchi yozuv.",
            "source": "demo",
            "lang": "uz",
        },
    ]
)
os.makedirs("./data/parquet", exist_ok=True)
df.to_parquet("./data/parquet/train-00001.parquet", index=False)
