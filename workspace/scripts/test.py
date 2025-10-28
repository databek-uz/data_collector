from datasets import load_dataset

dataset = load_dataset("databek/uzbek_news_site")
print(dataset["train"][0])
