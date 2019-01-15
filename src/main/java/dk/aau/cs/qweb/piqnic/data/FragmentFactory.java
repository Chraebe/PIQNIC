package dk.aau.cs.qweb.piqnic.data;

import dk.aau.cs.qweb.piqnic.data.impl.PredicateSpecificFragment;
import dk.aau.cs.qweb.piqnic.data.impl.PredicateSpecificMetaFragment;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;

import java.io.File;
import java.util.Set;

public class FragmentFactory {
    public static FragmentBase createFragment(String baseUri, String id, File file, Peer owner) {
        return new PredicateSpecificFragment(baseUri, id, file, owner);
    }

    public static MetaFragmentBase createMetaFragment(String baseUri, String id, File file, Peer owner, Set<IPeer> clients) {
        return new PredicateSpecificMetaFragment(baseUri, id, file, owner, clients);
    }
}
