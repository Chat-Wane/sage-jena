package fr.gdd.volcano;

import org.apache.jena.tdb2.sys.InitTDB2;



public class InitSage extends InitTDB2 {

    @Override
    public void start() {
        System.out.println("InitSage.start()");
        super.start();
        // StageGenerator parent = (StageGenerator)ARQ.getContext().get(ARQ.stageGenerator) ;
        // SageStageGenerator sageStageGenerator = new SageStageGenerator(parent, backend);

        
    }

    @Override
    public void stop() {
        System.out.println("InitSage.stop()");
        super.stop();
    }

}
