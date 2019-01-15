package dk.aau.cs.qweb.piqnic.data;

import com.google.gson.Gson;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public abstract class FragmentBase implements IFragment {
    private final String baseUri;
    private final String id;
    private final File file;
    private final Peer owner;

    public FragmentBase(String baseUri, String id, File file, Peer owner) {
        this.baseUri = baseUri;
        this.id = id;
        this.file = file;
        this.owner = owner;
    }

    public Peer getOwner() {
        return owner;
    }

    public String getBaseUri() {
        return baseUri;
    }

    public String getId() {
        return id;
    }

    public File getFile() {
        return file;
    }

    public String getIdString() {
        return baseUri + ":" + id;
    }

    public boolean ownedBy(Peer peer) {
        return owner.equals(peer);
    }

    public MetaFragmentBase toMetaFragment(Set<IPeer> peers) {
        return FragmentFactory.createMetaFragment(baseUri, id, file, owner, peers);
    }

    @Override
    public String toString() {
        return toJSONString();
    }

    private String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FragmentBase that = (FragmentBase) o;
        return Objects.equals(baseUri, that.baseUri) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseUri, id);
    }
}
