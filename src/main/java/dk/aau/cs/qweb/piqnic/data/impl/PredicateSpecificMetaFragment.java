package dk.aau.cs.qweb.piqnic.data.impl;

import com.google.gson.Gson;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.util.Triple;

import java.io.File;
import java.util.Set;

public class PredicateSpecificMetaFragment extends MetaFragmentBase {
    public PredicateSpecificMetaFragment(String baseUri, String id, File file, Set<IPeer> clients) {
        super(baseUri, id, file, clients);
    }

    @Override
    public boolean identify(Triple triplePattern) {
        return triplePattern.getPredicate().equals("ANY")
                || triplePattern.getPredicate().startsWith("?")
                || triplePattern.getPredicate().equals(getId());
    }

    @Override
    public String toString() {
        return toJSONString();
    }

    private String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
