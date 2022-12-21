package fr.gdd;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;

import static org.apache.jena.tdb2.sys.SystemTDB.SizeOfNodeId;

import org.apache.jena.atlas.iterator.NullIterator;
import org.apache.jena.atlas.iterator.SingletonIterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.base.record.RecordMapper;
import org.apache.jena.dboe.index.RangeIndex;
import org.apache.jena.dboe.trans.bplustree.BPTreeNode;
import org.apache.jena.dboe.trans.bplustree.BPlusTree;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.NodeIdFactory;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;



public class PreemptableTupleIndexRecord {

    RecordFactory factory;
    RangeIndex index;
    RecordMapper<Tuple<NodeId>> recordMapper;
    TupleIndexRecord tir;
    
    public PreemptableTupleIndexRecord(TupleIndexRecord tir) {
        Field factoryField = ReflectionUtils._getField(TupleIndexRecord.class, "factory");
        this.factory = (RecordFactory) ReflectionUtils._callField(factoryField, tir.getClass(), tir);
        Field indexField = ReflectionUtils._getField(TupleIndexRecord.class, "index");
        this.index = (RangeIndex) ReflectionUtils._callField(indexField, tir.getClass(), tir);
        Field recordMapperField = ReflectionUtils._getField(TupleIndexRecord.class, "recordMapper");
        this.recordMapper = (RecordMapper<Tuple<NodeId>>) ReflectionUtils._callField(recordMapperField, tir.getClass(), tir);
        this.tir = tir;
    }

    public Iterator<Tuple<NodeId>> scan(Tuple<NodeId> patternNaturalOrder) {
        Tuple<NodeId> pattern = this.tir.getMapping().map(patternNaturalOrder);
        
        // Canonical form.
        int numSlots = 0;
        int leadingIdx = -2;    // Index of last leading pattern NodeId.  Start less than numSlots-1
        boolean leading = true;

        // Records.
        Record minRec = factory.createKeyOnly();
        Record maxRec = factory.createKeyOnly();

        // Set the prefixes.
        for ( int i = 0; i < pattern.len() ; i++ ) {
            NodeId X = pattern.get(i);
            if ( NodeId.isAny(X) ) {
                X = null;
                // No longer seting leading key slots.
                leading = false;
                continue;
            }
            // if ( NodeId.isDoesNotExist(X) )
            //     return Iter.nullIterator();

            numSlots++;
            if ( leading ) {
                leadingIdx = i;
                NodeIdFactory.set(X, minRec.getKey(), i*SizeOfNodeId);
                NodeIdFactory.set(X, maxRec.getKey(), i*SizeOfNodeId);
            }
        }

        // Is it a simple existence test?
        if ( numSlots == pattern.len() ) {
            if ( index.contains(minRec) )
                return new SingletonIterator<>(pattern);
            else
                return new NullIterator<>();
        }
        
        Iterator<Tuple<NodeId>> tuples;
        if ( leadingIdx < 0 ) {
            // fullScan always allowed
            // if ( ! fullScanAllowed )
            // return null;
            // Full scan necessary
            tuples = index.iterator(null, null, recordMapper);
        } else {
            // Adjust the maxRec.
            NodeId X = pattern.get(leadingIdx);
            // Set the max Record to the leading NodeIds, +1.
            // Example, SP? inclusive to S(P+1)? exclusive where ? is zero.
            NodeIdFactory.setNext(X, maxRec.getKey(), leadingIdx*SizeOfNodeId);

            tuples = index.iterator(minRec, maxRec, recordMapper);

            RangeIndex rIndex = tir.getRangeIndex();
            BPlusTree bpt = (BPlusTree) rIndex;

            Method startReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class, "startReadBlkMgr");
            ReflectionUtils._callMethod(startReadBlkMgrMethod, bpt.getClass(), bpt);
            Method getRootReadMethod = ReflectionUtils._getMethod(BPlusTree.class, "getRootRead");
            BPTreeNode root = (BPTreeNode) ReflectionUtils._callMethod(getRootReadMethod, bpt.getClass(), bpt);
            Method releaseRootReadMethod = ReflectionUtils._getMethod(BPlusTree.class, "releaseRootRead", BPTreeNode.class);
            ReflectionUtils._callMethod(releaseRootReadMethod, bpt.getClass(), bpt, root);
            Method finishReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class, "finishReadBlkMgr");
            ReflectionUtils._callMethod(finishReadBlkMgrMethod, bpt.getClass(), bpt);
            
            tuples = BPTreePreemptableRangeIterator.create(root, minRec, maxRec, recordMapper);
            
            // in org.apache.jena.dboe.trans.bplustree.BPlusTree.java
            // startReadBlkMgr();
            // BPTreeNode root = getRootRead();
            // releaseRootRead(root);
            // finishReadBlkMgr();
            // return iterator(root, minRec, maxRec, mapper);

            // int keyLen = recordsMgr.getRecordBufferPageMgr().getRecordFactory().keyLength();
            // return BPTreeRangeIteratorMapper.create(node, minRec, maxRec, keyLen, mapper);
 
            
        }
        
        if ( leadingIdx < numSlots-1 ) {
            // partial scan always allowed
            // if ( ! partialScanAllowed )
            // return null;
            // Didn't match all defined slots in request.
            // Partial or full scan needed.
            //pattern.unmap(colMap);
            Method scanMethod = ReflectionUtils._getMethod(TupleIndexRecord.class, "scan");
            // tuples = scan(tuples, patternNaturalOrder);
            tuples = (Iterator<Tuple<NodeId>>) ReflectionUtils._callMethod(scanMethod, tir.getClass(), tir,
                                                                           tuples, patternNaturalOrder);
        }
        
        return tuples;

        // Deadcode after this line, we skip it for now:
        // <https://github.com/apache/jena/blob/ebc10c4131726e25f6ffd398b9d7a0708aac8066/jena-tdb2/src/main/java/org/apache/jena/tdb2/store/tupletable/TupleIndexRecord.java#L216>
        
    }

}
