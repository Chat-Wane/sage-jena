package fr.gdd.volcano;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.tdb2.sys.InitTDB2;
import org.apache.jena.sparql.engine.main.QC;

import fr.gdd.jena.JenaBackend;



public class InitSage extends InitTDB2 {

    @Override
    public void start() {
        System.out.println("InitSage.start()");
        super.start();
        // StageGenerator parent = (StageGenerator)ARQ.getContext().get(ARQ.stageGenerator) ;
        // String datasetPath = "/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M";
        // JenaBackend backend = new JenaBackend(datasetPath);
        
        // SageStageGenerator sageStageGenerator = new SageStageGenerator(parent, backend);
        // StageBuilder.setGenerator(ARQ.getContext(), sageStageGenerator);
        // SageOpExecutorFactory sageFactory = new SageOpExecutorFactory();
        // QC.setFactory(ARQ.getContext(), sageFactory);
    }

    @Override
    public void stop() {
        System.out.println("InitSage.stop()");
        super.stop();
    }

}
