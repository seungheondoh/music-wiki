import os
import json
import jsonlines
from concurrent.futures import ThreadPoolExecutor, as_completed
from datasets import Dataset, DatasetDict
from tqdm import tqdm
import wikipediaapi

wiki_wiki = wikipediaapi.Wikipedia(
        user_agent='bulid music wiki',
        language='en',
        extract_format=wikipediaapi.ExtractFormat.WIKI
)

def data_loader(mb_path, entity):
    datas = []
    data_file = os.path.join(mb_path, "parsing", f"{entity}.jsonl")
    with jsonlines.open(data_file) as f:
        for line in f.iter():
            line["entity_type"] = entity
            datas.append(line)
    return datas

def get_wiki(instance):
    page_py = wiki_wiki.page(instance["text"])
    if (page_py.exists()) and (page_py.text):
        return {
            "mbid" : instance['mbid'],
            "entity" : page_py.title,
            'text' : page_py.text,
            "url" : page_py.fullurl,
            "entity_type": instance['entity_type']
        }
        
if __name__ == "__main__":
    mb_path = "../datasets/musicbrainz"
    wiki_path = "../datasets/wikipedia"
    entity_list = ['genre','instrument', 'artist', 'event', 'label', 'place', 'release', 'release_group', 'series', 'url', 'work', 'area', 'recording']
    for entity in entity_list:
        datas = data_loader(mb_path, entity)
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(get_wiki, instance)
                for instance in datas
            ]
            results = [item.result() for item in tqdm(as_completed(futures), total=len(datas), desc=None) if item.result()]
        with open(os.path.join(wiki_path, f"{entity}_data.jsonl"), encoding= "utf-8",mode="w") as f: 
            for i in results: f.write(json.dumps(i) + "\n")
        print(entity)
        print(len(results), len(datas), len(results)/len(datas))
        print("="*20)