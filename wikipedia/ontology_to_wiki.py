import os
import json
import jsonlines
import wikipediaapi
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# make global variable
wiki_wiki = wikipediaapi.Wikipedia(
    user_agent='bulid music wiki',
    language='en',
    extract_format=wikipediaapi.ExtractFormat.WIKI
)
global MUSIC_ENTITY
MUSIC_ENTITY = []

def recursive_music_entity(categorymembers, level=0, max_level=16):
    for c in categorymembers.values():
        MUSIC_ENTITY.append({"title":  c.title, "ns": c.ns})
        if c.ns == wikipediaapi.Namespace.CATEGORY and level < max_level:
            level = level + 1
            recursive_music_entity(c.categorymembers, level=level, max_level=max_level)

def get_entity(wiki_path):
    cat = wiki_wiki.page("Category:Music")
    recursive_music_entity(cat.categorymembers)
    with open(os.path.join(wiki_path, f"entity.jsonl"), encoding= "utf-8",mode="w") as f: 
        for i in MUSIC_ENTITY: f.write(json.dumps(i) + "\n")

def get_wiki(instance):
    page_py = wiki_wiki.page(instance["title"])
    if (page_py.exists()) and (page_py.text):
        return {
            "mbid" : "",
            "entity" : page_py.title,
            'text' : page_py.text,
            "url" : page_py.fullurl,
            "entity_type": "wiki_ontology"
        }

if __name__ == "__main__":
    wiki_path = "../datasets/wikipedia"
    get_entity(wiki_path)
    print(len(MUSIC_ENTITY))
    music_entity = MUSIC_ENTITY.copy()
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(get_wiki, instance)
            for instance in music_entity
        ]
        results = [item.result() for item in tqdm(as_completed(futures), total=len(music_entity), desc=None) if item.result()]

    with open(os.path.join(wiki_path, f"wiki_data.jsonl"), encoding= "utf-8",mode="w") as f: 
        for i in results: f.write(json.dumps(i) + "\n")

    print(len(results), len(music_entity), len(results)/len(music_entity))