package dk.aau.cs.qweb.pweb.node;

import dk.aau.cs.qweb.pweb.peer.IPeer;
import dk.aau.cs.qweb.pweb.peer.Peer;
import dk.aau.cs.qweb.pweb.util.Fragment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public abstract class AbstractNode implements INode {
    final UUID id;
    final String ip;
    final int port;
    public final int maxFragments = 200;

    ArrayList<Fragment> fragments = new ArrayList<>();
    ArrayList<String> fragmentKeys = new ArrayList<>();
    ArrayList<IPeer> peers = new ArrayList<>();

    public void setPeers(ArrayList<IPeer> peers) {
        this.peers = peers;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getIp() {
        return ip;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public void addPeer(IPeer peer) {
        peers.add(peer);
    }

    @Override
    public void addPeers(List<IPeer> peers) {
        peers.addAll(peers);
    }

    @Override
    public void addFragments(List<Fragment> fragments) throws IOException {
        for(Fragment f : fragments) {
            addFragment(f);
        }
        reloadDatastore();
    }

    @Override
    public List<Fragment> getRandomFragments(int num) {
        List<Fragment> ret = new ArrayList<>();
        if(fragments.size() == 0) return ret;

        Random rand = new Random();
        for(int i = 0; i < num; i++) {
            ret.add(fragments.get(rand.nextInt(fragments.size()-1)));
        }

        return ret;
    }

    @Override
    public List<Peer> getRandomPeers(int num) {
        List<Peer> ret = new ArrayList<>();
        if(peers.size() == 0) return ret;

        Random rand = new Random();
        for(int i = 0; i < num; i++) {
            ret.add((Peer)peers.get(rand.nextInt(peers.size()-1)));
        }

        return ret;
    }

    @Override
    public List<IPeer> getPeers() {return peers;}

    public AbstractNode(String ip, int port) {
        id = UUID.randomUUID();
        this.ip = ip;
        this.port = port;
    }

    @Override
    public int fragmentCount() {
        return fragments.size();
    }
}
