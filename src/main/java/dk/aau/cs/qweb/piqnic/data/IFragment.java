package dk.aau.cs.qweb.piqnic.data;

import dk.aau.cs.qweb.piqnic.util.Triple;

public interface IFragment {
    boolean identify(Triple triplePattern);
}
