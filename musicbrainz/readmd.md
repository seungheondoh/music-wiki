# MusicBrainz Preprocessor

1. Download dump data.
    - mbdump data: https://ftp.osuosl.org/pub/musicbrainz/data/fullexport/20230809-002404/mbdump.tar.bz2

2. Follow db schema
    - db schema parsing: https://musicbrainz.org/doc/MusicBrainz_Database/Schema
    - parsing data by: https://wiki.musicbrainz.org/images/4/46/entity_network_details.svg

3. Get entity category
    - entity.json from [MusicBrainz_Entity](https://wiki.musicbrainz.org/MusicBrainz_Entity)

4. Save entity data

```
python get music_entity.py
```