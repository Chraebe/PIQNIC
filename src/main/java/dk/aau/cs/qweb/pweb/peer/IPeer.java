package dk.aau.cs.qweb.pweb.peer;

import dk.aau.cs.qweb.pweb.util.Fragment;
import dk.aau.cs.qweb.pweb.util.QueryResult;

import java.io.IOException;
import java.util.List;

public interface IPeer {
    QueryResult query(String sparql, int timeToLive) throws IOException;

    String exchangeInformation(List<Peer> peers, List<Fragment> fragments) throws IOException;

    String addFragment(Fragment fragment, int timeToLive) throws IOException;

    void reloadDatastore() throws IOException;
}
