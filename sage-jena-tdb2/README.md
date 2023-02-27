# Sage ⨯ Jena-TDB2

[TDB2](https://github.com/apache/jena/tree/main/jena-tdb2) is the
default data storage kindly provided by [Apache
Jena](https://github.com/apache/jena). Under the hood, indexes of RDF
quads and/or RDF triples are stored in [balanced plus
trees](https://en.wikipedia.org/wiki/B%2B_tree). This project enhances
Jena by providing:

- [X] The additional ability to set the departure of a range iterator
  using its `BPlusTree` keys, hence enabling pausing/resuming of query
  execution: when pausing, the last key of a range iterator is saved;
  on resuming, the saved key is used to create a new range iterator
  departing from this key.
  
- [X] A way to create randomly jumping range iterators where each call
  to `next()` produces a random value within the initial range by
  descending in the `BPlusTree` at random. This is ideal to create
  samples.





