package fr.gdd.sage.arq;

import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.tdb2.store.GraphViewSwitchable;
import org.apache.jena.tdb2.solver.PatternMatchSage;



/**
 * Class in charge of creating preemptable basic graph patterns
 * (BGPs).
 * 
 * comes from:
 * <https://github.com/apache/jena/blob/main/jena-tdb2/src/main/java/org/apache/jena/tdb2/solver/StageGeneratorDirectTDB.java#L45>
 */
public class SageStageGenerator implements StageGenerator {
    StageGenerator parent;
    
    public SageStageGenerator(StageGenerator parent) {
        this.parent = parent;
    }
    
    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {

        Graph g = execCxt.getActiveGraph();
        if (g instanceof GraphViewSwitchable) {
            GraphViewSwitchable gvs = (GraphViewSwitchable)g;
            g = gvs.getBaseGraph();
        }
        if (!(g instanceof GraphTDB)) {
            return parent.execute(pattern, input, execCxt);
        }
        
        return PatternMatchSage.matchTriplePattern(pattern, input, execCxt);
    }

}
