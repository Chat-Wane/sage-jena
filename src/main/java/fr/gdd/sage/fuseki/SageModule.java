package fr.gdd.sage.fuseki;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.fuseki.server.Endpoint;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.fuseki.servlets.ActionService;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.tdb2.DatabaseMgr;
import org.apache.jena.tdb2.TDB2;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.arq.SageOpExecutor;
import fr.gdd.sage.arq.SageStageGenerator;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.writers.SageRowSetWriterJSON;



/**
 * Module in charge of replacing fuseki's normal behavior for `query`
 * by one that enables preemptive evaluation of queries, i.e. one that
 * enables pausing/resuming of query execution on demand, depending on
 * arguments in http headers.
 * 
 * The module simply sets the processor of `Operation.QUERY` to ours,
 * for every dataset and endpoint.
 *
 * For this to work, either set full class name in a
 * `META-INF/…/…FusekiModule` file as per say in documentation, or
 * work with `addModule`.
 */
public class SageModule implements FusekiModule {
    Logger logger = LoggerFactory.getLogger(SageModule.class);

    public SageModule() {}
    
    @Override
    public String name() {
        return "Sage";
    }
    
    @Override
    public void start() {
        logger.info("start !");
        // replace the default engine behavior by ours.
        QC.setFactory(ARQ.getContext(), SageOpExecutor.factory);
        StageGenerator parent = (StageGenerator) ARQ.getContext().get(ARQ.stageGenerator) ;
        SageStageGenerator sageStageGenerator = new SageStageGenerator(parent);
        StageBuilder.setGenerator(ARQ.getContext(), sageStageGenerator);

        // replace the output by ours to include the saved state.
        // all writers are here : <https://github.com/apache/jena/tree/main/jena-arq/src/main/java/org/apache/jena/riot/rowset/rw>
        // (TODO) get them all
        RowSetWriterRegistry.register(ResultSetLang.RS_JSON, SageRowSetWriterJSON.factory);
    }

    /**
     * Server starting - called just before server.start happens.
     */
    @Override
    public void serverBeforeStarting(FusekiServer server) {
        logger.info("Patching the processor for query operations…");

        var dapr = server.getDataAccessPointRegistry();

        for (var dap : dapr.accessPoints()) {
            if (DatabaseMgr.isTDB2(dap.getDataService().getDataset())) {
                // register the new executors for every dataset that is TDB2
                Dataset ds =  DatasetFactory.wrap(dap.getDataService().getDataset());
                QC.setFactory(ds.getContext(), SageOpExecutor.factory);
                StageGenerator parent = (StageGenerator) ds.getContext().get(ARQ.stageGenerator) ;
                SageStageGenerator sageStageGenerator = new SageStageGenerator(parent);
                StageBuilder.setGenerator(ds.getContext(), sageStageGenerator);
                // to conveniently get interface changes already implemented
                JenaBackend backend = new JenaBackend(ds);
                ds.getContext().set(SageConstants.backend, backend);
            };
            
            // replacing the operation registry and the processor
            server.getOperationRegistry().register(Operation.Query, new Sage_QueryDataset());
            for (Endpoint ep : dap.getDataService().getEndpoints(Operation.Query)) {
                ep.setProcessor(server.getOperationRegistry().findHandler(ep.getOperation()));
            }
        }        
    }
    
    @Override
    public void serverStopped(FusekiServer server) {
        // (TODO) maybe put back the default behavior        
    }

    @Override
    public void stop() {
        logger.info("Stop! Have a good day!");
    }

    @Override
    public int level() {
        // (TODO) find out the proper level for this module.
        return 999999;
    }
}
