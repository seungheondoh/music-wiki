import os
import json
from datasets import Dataset, DatasetDict
from tqdm import tqdm

def data_parsing(data_file):
    datas = []
    with open(data_file, 'r', encoding='utf-8') as file:
        for line in file:
            data = line.strip().split('\t')
            datas.append({
                "id": data[0],
                "mbid": data[1],
                "text": data[2]
            })
    return datas

if __name__ == "__main__":
    mb_path = "../datasets/musicbrainz"
    entity = json.load(open(os.path.join(mb_path, "entity.json"), 'r'))
    entity_list = list(entity.keys())
    entity2item = {}
    for e_name in tqdm(entity_list):
        datas = data_parsing(os.path.join(mb_path, "mbdump", e_name))
        os.makedirs(os.path.join(mb_path, "parsing"), exist_ok=True)
        with open(os.path.join(mb_path, "parsing", f"{e_name}.jsonl"), encoding= "utf-8",mode="w") as f: 
            for i in datas: f.write(json.dumps(i) + "\n")
    print("finish")