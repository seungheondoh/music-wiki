import os
import json
import jsonlines
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from tqdm import tqdm
import wikipediaapi
import argparse
import time
wiki_wiki = wikipediaapi.Wikipedia(
        user_agent='bulid music wiki v1.',
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
    try:
        page_py = wiki_wiki.page(instance["text"])
        if (page_py.exists()) and (page_py.text):
            return {
                "mbid" : instance['mbid'],
                "entity" : page_py.title,
                'text' : page_py.text,
                "url" : page_py.fullurl,
                "entity_type": instance['entity_type']
            }
    except:
        return None

def main(args):
    datas = data_loader(args.mb_path, args.entity)
    save_path = os.path.join(args.wiki_path, f"{args.entity}_data.jsonl")
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(get_wiki, instance)
            for instance in datas
        ]
        results = [future.result()  for future in tqdm(as_completed(futures), total=len(datas), desc=None) if future.result()]
    with open(save_path, encoding= "utf-8",mode="w") as f: 
        for i in results: f.write(json.dumps(i) + "\n")
    print(args.entity)
    print(len(datas), len(results), len(results)/len(datas))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='crawling entity_to_wiki')
    parser.add_argument('--mb_path', type=str, default="../datasets/musicbrainz")
    parser.add_argument('--wiki_path', type=str, default="../datasets/wikipedia")
    parser.add_argument('--entity', type=str, default="genre")
    # ['genre','instrument', 'artist', 'release', 'release_group', 'event', 'label', 'place', 'series', 'url', 'work', 'area', 'recording']
    args = parser.parse_args()
    main(args)