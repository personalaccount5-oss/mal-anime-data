# MAL Anime Offline Database Cache

Automated public cache of the MyAnimeList anime database for offline catalog browsing in Animetail.

## Available Datasets
- **Manifest**: [`manifest.json`](https://raw.githubusercontent.com/personalaccount5-oss/mal-anime-data/main/manifest.json)
- **Full Database (SQLite GZIP)**: [`mal_catalog.db.gz`](https://raw.githubusercontent.com/personalaccount5-oss/mal-anime-data/main/mal_catalog.db.gz)
- **Daily Airing Delta (JSON)**: [`airing_delta.json`](https://raw.githubusercontent.com/personalaccount5-oss/mal-anime-data/main/airing_delta.json)

## Update Schedule
- **Daily Airing Update**: Every day at `06:00 UTC` via GitHub Actions (`airing_delta.json`).
- **Weekly Full Catalog Build**: Every Sunday at `04:00 UTC` (`mal_catalog.db.gz`).

Built with open-source metadata from [manami-project/anime-offline-database](https://github.com/manami-project/anime-offline-database) and official MyAnimeList API v2.
