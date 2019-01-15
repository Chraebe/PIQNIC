package dk.aau.cs.qweb.piqnic.data;

import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;
import dk.aau.cs.qweb.piqnic.util.Triple;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public abstract class MetaFragmentBase extends FragmentBase {
    private Set<IPeer> clients;

    public MetaFragmentBase(String baseUri, String id, File file, Peer owner, Set<IPeer> clients) {
        super(baseUri, id, file, owner);
        this.clients = clients;
    }

    public Set<IPeer> getClients() {
        return clients;
    }

    public boolean hasPeer(IPeer peer) {
        return clients.contains(peer);
    }

    public void removePeer(IPeer peer) {
        clients.remove(peer);
    }

    public void addPeer(IPeer peer) {
        clients.add(peer);
    }

    public void addPeers(List<IPeer> peers) {
        clients.addAll(peers);
    }

    public String getPeerString() {
        String s = "[ ";

        for (IPeer p : clients) {
            s += p.getAddress() + ":" + p.getPort() + " ";
        }
        s += "]";
        return s;
    }

    public void addTriples(List<Triple> triples) {
        for(IPeer peer : clients) {
            try {
                peer.addTriplesToFragment(this, triples);
            } catch (IOException e) {}
        }
    }

    public void removeTriples(List<Triple> triples) {
        for(IPeer peer : clients) {
            try {
                peer.removeTriplesFromFragment(this, triples);
            } catch (IOException e) {}
        }
    }
}
