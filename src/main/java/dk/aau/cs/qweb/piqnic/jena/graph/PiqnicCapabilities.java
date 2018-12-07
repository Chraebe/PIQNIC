package dk.aau.cs.qweb.piqnic.jena.graph;

import org.apache.jena.graph.Capabilities;

public class PiqnicCapabilities implements Capabilities {
    @Override
    public boolean sizeAccurate() { return true; }
    @Override
    public boolean addAllowed() { return false; }
    @Override
    public boolean addAllowed( boolean every ) { return false; }
    @Override
    public boolean deleteAllowed() { return false; }
    @Override
    public boolean deleteAllowed( boolean every ) { return false; }
    @Override
    public boolean canBeEmpty() { return true; }
    @Override
    public boolean iteratorRemoveAllowed() { return false; }
    @Override
    public boolean findContractSafe() { return true; }
    @Override
    public boolean handlesLiteralTyping() { return true; }
}
