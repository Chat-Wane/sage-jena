# Sage ⨯ Jena Volcano

Jena's engine relies on a paradigm called `Volcano` (also known as
iterator model). [Jena
ARQ](https://jena.apache.org/documentation/query/index.html) allows
practitioners to easily modify the chain of executions that represents
a query.

This project creates the factories to include preemptive iterators
directly inside the volcano model.
