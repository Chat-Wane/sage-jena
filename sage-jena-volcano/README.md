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

To implement the preemptable operators, the volcano must be breakable
when conditions (timeout and limit) are met, by the mean of `try
catch` and `throw`. For the sake of simplicity, only scan iterators
are allowed to throw, and the root of iterators `PreemptRootIter` is
able to `catch` and save every preemptable operator. As a consequence,
implementing other preemptable operators mainly consists in proper
initialization.

## Implementing Preemptable Operators

When one wants to implement a new preemptable operator, there are a
few steps to follow:

- Make sure that the operator gets a **unique identifier** in
  `IdentifierAllocator`. This identifier serves to map each operator
  with its corresponding instance at runtime. Therefore, at pausing
  time, the additional metadata are saved by calling the physical
  instance of each operator; and at resuming time, each physical
  operator skips parts of its computation by using its saved metadata
  (*granted that the logical plan remains the same between pauses and
  resumes*).
  
- Make sure that identifiers are linked together and resembles the
  tree of execution in `IdentifierLinker`. For instance, considering
  `tp1.tp2` with `id(tp1)=1`, and `id(tp2)=2`, then 1 is the parent
  of 2. This gets more complex with operators such as `UNION` or
  `OPTIONAL`.

- <i> (Note) The two steps above could probably be implemented for all
  operators at once, i.e., even operators that are not preemptable
  would get their unique identifier.  This would alleviate the future
  developer from such a painful task.</i>

- Actually implement the operator. It often consists of 2 classes:

    - the "instanciator" which takes the operator and execution
      context in argument to instantiate actual iterators later
      on. Examples of such classes are `PreemptQueryIterOptionalIndex`,
      or `PreemptQueryIterUnion`.

    - the "instantiated" which takes its unique identifier as argument,
      that must implement the `PreemptIterator` interface, and that
      saves additional metadata. Examples of such classes are
      `PreemptQueryIterDefaulting` and `PreemptQueryIterConcat`.
