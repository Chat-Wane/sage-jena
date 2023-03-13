package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;

import static org.apache.jena.tdb2.sys.SystemTDB.SizeOfNodeId;
import org.apache.jena.atlas.iterator.NullIterator;
import org.apache.jena.atlas.iterator.SingletonIterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.base.record.RecordMapper;
import org.apache.jena.dboe.index.RangeIndex;
import org.apache.jena.dboe.trans.bplustree.BPTreeNode;
import org.apache.jena.dboe.trans.bplustree.BPlusTree;
import org.apache.jena.dboe.trans.bplustree.JenaIterator;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.NodeIdFactory;
import org.apache.jena.tdb2.store.tupletable.TupleIndexBase;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;



public class PreemptableTupleIndexRecord {

    RecordFactory factory;
    RangeIndex index;
    TupleMap tupleMap;
    
    public TupleIndexRecord tir;


    
    public PreemptableTupleIndexRecord(TupleIndexRecord tir) {
        Field factoryField = ReflectionUtils._getField(TupleIndexRecord.class, "factory");
        this.factory = (RecordFactory) ReflectionUtils._callField(factoryField, tir.getClass(), tir);
        Field tupleMapField = ReflectionUtils._getField(TupleIndexBase.class, "tupleMap");
        this.tupleMap = (TupleMap) ReflectionUtils._callField(tupleMapField, TupleIndexBase.class, tir);
        
        this.index = tir.getRangeIndex(); 
        this.tir = tir;
    }

    public JenaIterator scan(Tuple<NodeId> patternNaturalOrder) {
        Tuple<NodeId> pattern = this.tir.getMapping().map(patternNaturalOrder);
        
        // Canonical form.
        int numSlots = 0;
        int leadingIdx = -2;  // Index of last leading pattern NodeId.  Start less than numSlots-1
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
        // (TODO)
        // if ( numSlots == pattern.len() ) {
        //     if ( index.contains(minRec) )
        //         return new SingletonIterator<>(pattern);
        //     else
        //         return new NullIterator<>();
        // }
        
        // Iterator<Tuple<NodeId>> tuples;
        JenaIterator tuples;
        if ( leadingIdx < 0 ) {
            // fullScan always allowed
            // if ( ! fullScanAllowed )
            // return null;
            // Full scan necessary
            // tuples = index.iterator(null, null, recordMapper);
            // (TODO)(TODO)(TODO)(TODO)(TODO)(TODO)
            // tuples = new JenaIterator(tupleMap, null, minRec, maxRec);
            tuples = null;
        } else {
            // Adjust the maxRec.
            NodeId X = pattern.get(leadingIdx);
            // Set the max Record to the leading NodeIds, +1.
            // Example, SP? inclusive to S(P+1)? exclusive where ? is zero.
            NodeIdFactory.setNext(X, maxRec.getKey(), leadingIdx*SizeOfNodeId);

            // tuples = index.iterator(minRec, maxRec, recordMapper);

            RangeIndex rIndex = tir.getRangeIndex();
            BPlusTree bpt = (BPlusTree) rIndex;
            
            // in org.apache.jena.dboe.trans.bplustree.BPlusTree.java
            bpt.startReadBlkMgr();
            int rootId = bpt.getRootId();
            BPTreeNode root = bpt.getNodeManager().getRead(rootId, BPlusTreeParams.RootParent);
            root.release();
            bpt.finishReadBlkMgr();
            tuples = new JenaIterator(tupleMap, root, minRec, maxRec);
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
            // tuples = (Iterator<Tuple<NodeId>>) ReflectionUtils._callMethod(scanMethod, tir.getClass(), tir,
            // tuples, patternNaturalOrder);
            // (TODO)
            tuples = null;
        }
        
        return tuples;
        
    }

}
