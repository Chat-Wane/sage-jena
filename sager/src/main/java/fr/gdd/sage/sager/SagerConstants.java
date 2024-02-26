package fr.gdd.sage.sager;

import org.apache.jena.sparql.util.Symbol;

public class SagerConstants {

    public static final String systemVarNS = "https://sage.gdd.fr/Sager#";
    public static final String sageSymbolPrefix = "sager";

    static public final Symbol BACKEND = allocVariableSymbol("Backend");
    static public final Symbol LOADER = allocVariableSymbol("Loader");
    static public final Symbol SAVER = allocVariableSymbol("Saver");

    static public final Symbol TIMEOUT = allocConstantSymbol("Timeout");
    static public final Symbol DEADLINE = allocConstantSymbol("Deadline");

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
