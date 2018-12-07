package dk.aau.cs.qweb.piqnic.data;

import dk.aau.cs.qweb.piqnic.peer.IPeer;

import java.io.File;
import java.util.List;
import java.util.Set;

public abstract class MetaFragmentBase extends FragmentBase {
    private Set<IPeer> clients;

    public MetaFragmentBase(String baseUri, String id, File file, Set<IPeer> clients) {
        super(baseUri, id, file);
        this.clients = clients;
    }

    public Set<IPeer> getClients() {
        return clients;
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
}
