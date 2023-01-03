package fr.gdd;

public interface BackendIterator<ID, SKIP> {
    
    /**
     * @param variable_name The name of the variable to look for, for
     * instance: `?subject`.
     * @return The identifier by variable name.
     */
    default ID getId(final String variable_name) {return null;}

    /**
     * @param variable_name The name of the variable to look for,
     * for instance: `?subject`.
     * @return the value by variable name.
     */
    default String getLiteral(final String variable_name) {return null;}
    
    /**
     * @return The current subject id.
     */
    default ID subjectId () { return null; }
    
    /**
     * @return The current predicate id.
     */
    default ID predicateId () { return null; }
    
    /**
     * @return The current object id.
     */
    default ID objectId () { return null; }
    
    /**
     * @return The current context (or graph) id.
     */
    default ID contextId () { return null; }
    
    /**
     * @return true if there are other elements matching the pattern,
     * false otherwise.
     */
    public boolean hasNext();

    /**
     * Iterates to the next element.
     */
    public void next();
    
    /**
     * Go back to the begining of the iterator. Enables reusing of
     * iterators.
     */
    public void reset();

    /**
     * Goes to the targeted element directly.
     * @param to The cursor location to skip to.
     */
    public void skip(final SKIP to);
    
    /**
     * @return The current offset that allows skipping.
     */
    public SKIP current();
    
    /**
     * @return The previous offset that allows skipping.
     */
    public SKIP previous();

    /**
     * @return The estimated cardinality of the iterator.
     */
    default long cardinality () { return -1; }


    // (TODO) how to specialize types ? 
    
    String subject();
    String predicate();
    String object();
    String context();
}
