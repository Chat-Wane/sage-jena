package fr.gdd.sage.arq;

import org.apache.jena.sparql.util.Symbol;



public class SageConstants {

    public static final String systemVarNS = "http://sage.gdd.fr/Sage#";
    public static final String sageSymbolPrefix = "sage";

    static public Symbol backend = allocConstantSymbol("Backend");
    
    static public Symbol timeout = allocConstantSymbol("Timeout");
    static public Symbol limit   = allocConstantSymbol("Limit");

    static public Symbol scanFactory = allocVariableSymbol("ScanFactory");
    static public Symbol deadline = allocVariableSymbol("Deadline");
    static public Symbol output   = allocVariableSymbol("Output");
    static public Symbol input    = allocVariableSymbol("Input");

    static public Symbol iterators = allocVariableSymbol("Iterators");


    
    /**
     * Symbol in use in the global context.
     */
    public static Symbol allocConstantSymbol(String name) {
        return Symbol.create(systemVarNS + name);
    }
    
    /**
     * Symbol in use in each execution context.
     */
    public static Symbol allocVariableSymbol(String name) {
        return Symbol.create(sageSymbolPrefix + name);
    }
}