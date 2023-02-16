# Sage ⨯ Jena

[Jena](https://jena.apache.org/) is `A free and open source Java
framework for building Semantic Web and Linked Data applications.`
This incredible piece of work includes all components to build an
end-to-end software to evaluate SPARQL Query over RDF graphs. 

[Sage](http://sage.univ-nantes.fr/) is an approach that enables
pausing/resuming query execution as long as the data storage allows
it.

This project provides the few additional components that enable Sage
on top of Jena. 

- [X] Data storage *TDB2* uses balanced plus tree that enables range
  queries. This project enhances it with the possibility to skip a
  range of values. 

- [X] Sage *FusekiModule* that intercepts the endpoint `query` operations
  to execute using enhanced operations, returning the results plus
  additional metadata that could be used to continue the execution
  later on.

- [ ] Modified *Jena Fuseki UI* that slightly changes end users' UI so
  they can configure their query executions, `id est`, whether they
  want an automatic re-execution of their query up till termination,
  whether they want random samples, etc.
