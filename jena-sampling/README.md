# Jena ⨯ Sampling

*Jena* allows users to execute a query from start to finish, granted
it finishes before a preset timeout.  *Sage ⨯ Jena* allows users to
execute a query from start to finish whatever the timeout by
pausing/resuming query execution for multiple rounds.

*Jena ⨯ Sampling* extends Jena by providing sampling capabilities to
users: 

- [ ] A request does not simply execute but provides a random view of
  the query execution. In other words, it returns a tree of random
  walks representing a random subset of the full execution tree. This
  enables query optimizations with the idea that spending time in
  exploring will result in massive execution time gains. 
