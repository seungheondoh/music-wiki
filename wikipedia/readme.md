
- wikipeida: wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2

```
python -m wikiextractor.WikiExtractor ../datasets/wiki/enwiki-20230801-pages-articles-multistream.xml.bz2 -o ../datasets/wiki/enwiki-20230801.json --json
```