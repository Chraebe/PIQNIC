package dk.aau.cs.qweb.piqnic.data;

import dk.aau.cs.qweb.piqnic.data.impl.PredicateSpecificFragment;
import dk.aau.cs.qweb.piqnic.data.impl.PredicateSpecificMetaFragment;
import dk.aau.cs.qweb.piqnic.peer.IPeer;

import java.io.File;
import java.util.Set;

public class FragmentFactory {
    public static FragmentBase createFragment(String baseUri, String id, File file) {
        return new PredicateSpecificFragment(baseUri, id, file);
    }

    public static MetaFragmentBase createMetaFragment(String baseUri, String id, File file, Set<IPeer> clients) {
        return new PredicateSpecificMetaFragment(baseUri, id, file, clients);
    }
}
