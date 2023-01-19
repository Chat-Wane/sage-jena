package fr.gdd;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.util.iterator.ExtendedIterator;



public class SageGraph { // extends GraphTDB {

    public DatasetGraphTDB graph;
    
    public SageGraph(DatasetGraphTDB graph) {
        this.graph = graph;
    }

    // @Override
    // protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
    //     return null;
    //     // return graph.findInUnionGraph(triplePattern.getSubject(),
    //     //                               triplePattern.getPredicate(),
    //     //                               triplePattern.getObject())
    // }

}
