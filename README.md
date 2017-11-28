# lichess search import

This code streams lichess games from a mongodb secondary to the lichess-search service.

## Usage

```
# import since Jan 2016
sbt "run http://172.16.0.8:9822 2016-01"

# reset search index and import since Jan 2011
sbt "run http://172.16.0.8:9822 reset"
```
