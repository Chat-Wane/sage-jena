# Sage ⨯ Jena Volcano

Apache Jena's engine relies on a paradigm called **Volcano** (also
known as iterator model). [Jena
ARQ](https://jena.apache.org/documentation/query/index.html) allows
practitioners to easily modify the chain of executions that represents
a query. This project creates the necessary factories to include
preemptive iterators directly inside this volcano model. It ranges
from the basic triple/quad iterator (that comes from
[sage-jena-tdb2](https://github.com/Chat-Wane/sage-jena/tree/main/sage-jena-tdb2))
to iterators that implement operators such as `UNION`, or `OPTIONAL`.
