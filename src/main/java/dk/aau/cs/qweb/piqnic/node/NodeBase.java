package dk.aau.cs.qweb.piqnic.node;

import dk.aau.cs.qweb.piqnic.data.Dataset;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.util.Triple;

import java.io.IOException;
import java.util.*;

public abstract class NodeBase implements INode {
    final UUID id;
    final String ip;
    final int port;
    final List<Dataset> datasets = new ArrayList<>();
    final List<FragmentBase> fragments = new ArrayList<>();
    List<IPeer> neighbours = new ArrayList<>();

    public NodeBase(UUID id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
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
    public void addNeighbour(IPeer peer) {
        neighbours.add(peer);
    }

    @Override
    public void addNeighbours(List<IPeer> peers) {
        neighbours.addAll(peers);
    }

    @Override
    public void addFragments(List<FragmentBase> fragments) throws IOException {
        for (FragmentBase f : fragments) {
            addFragment(f);
        }
    }

    @Override
    public List<Dataset> getDatasets() {
        return this.datasets;
    }

    @Override
    public boolean insertFragment(FragmentBase fragmentBase) {
        synchronized (fragments) {
            for (FragmentBase f : fragments) {
                if (f.getBaseUri().equals(fragmentBase.getBaseUri()) && f.getId().equals(fragmentBase.getId()))
                    return false;
            }

            fragments.add(fragmentBase);
        }
        return true;
    }

    @Override
    public void addDataset(Dataset dataset) {
        datasets.add(dataset);
    }

    @Override
    public void addFragment(FragmentBase fragment) {
        fragments.add(fragment);
    }

    @Override
    public void removePeer(IPeer peer) {
        neighbours.remove(peer);
    }

    @Override
    public List<FragmentBase> getAllFragments() {
        return new ArrayList<>(this.fragments);
    }

    @Override
    public List<IPeer> getRandomPeers(int num) {
        List<IPeer> ret = new ArrayList<>();
        if (neighbours.size() == 0) return ret;
        if (neighbours.size() == 1) {
            List<IPeer> rl = new ArrayList<>();
            rl.add(neighbours.get(0));
            return rl;
        }

        Random rand = new Random();
        for (int i = 0; i < num; i++) {
            ret.add(neighbours.get(rand.nextInt(neighbours.size() - 1)));
        }

        return ret;
    }

    @Override
    public List<IPeer> getNeighbours() {
        return new ArrayList<>(neighbours);
    }

    NodeBase(String ip, int port) {
        id = UUID.randomUUID();
        this.ip = ip;
        this.port = port;
    }

    NodeBase(String ip, int port, UUID id) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    @Override
    public int fragmentCount() {
        return fragments.size();
    }
}
