import os
import torch
import random
import jsonlines
import argparse
from datasets import Dataset, DatasetDict

def load_jsonlines(data_file):
    datas = []
    with jsonlines.open(data_file) as f:
        for line in f.iter():
            datas.append(line)
    return datas

def main(args):
    all_corpus = {}
    all_tokens = 0
    for fname in os.listdir(args.wiki_path):
        if ".jsonl" in fname:
            dataset = load_jsonlines(os.path.join(args.wiki_path, fname))
            token_length = sum([len(i["text"].split()) for i in dataset])
            all_tokens += token_length
            print(fname, token_length)
            random.shuffle(dataset)
            all_corpus[fname.replace("_data.jsonl", "")] = dataset

    print(all_tokens)
            
    data_dict = {
        "wikipedia_music": Dataset.from_list(all_corpus["wiki"]),
        "musicbrainz_genre": Dataset.from_list(all_corpus["genre"]),
        "musicbrainz_instrument": Dataset.from_list(all_corpus["instrument"]),
        "musicbrainz_recording": Dataset.from_list(all_corpus["recording"]),
        "musicbrainz_artist": Dataset.from_list(all_corpus["artist"]),
        "musicbrainz_release": Dataset.from_list(all_corpus["release"]),
        "musicbrainz_release_group": Dataset.from_list(all_corpus["release_group"]),
        "musicbrainz_label": Dataset.from_list(all_corpus["label"]),
        "musicbrainz_work": Dataset.from_list(all_corpus["work"]),
        "musicbrainz_series": Dataset.from_list(all_corpus["series"]),
        "musicbrainz_place": Dataset.from_list(all_corpus["place"]),
        "musicbrainz_event": Dataset.from_list(all_corpus["event"]),
        "musicbrainz_area": Dataset.from_list(all_corpus["area"])
    }
    dd = DatasetDict(data_dict)
    # dd.push_to_hub("seungheondoh/music-wiki")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='bulid musicwiki')
    parser.add_argument('--wiki_path', type=str, default="./datasets/wikipedia/")
    args = parser.parse_args()
    main(args)