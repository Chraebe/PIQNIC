package dk.aau.cs.qweb.piqnic.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class Constituents{
    private Map<String, String> domainMap = new HashMap<>();
    private Map<String, Set<String>> uriMap = new HashMap<>();
    private int current = 0;

    public void addUri(String str) {
        String[] strs = str.split(";;");
        String uri = strs[0];
        String id = strs[1];

        URI u;
        try {
            u = new URI(uri);
        } catch(URISyntaxException e) {
            return;
        }

        String auth = uri.substring(0, uri.indexOf(u.getRawPath()));
        if(!domainMap.containsKey(auth)) {
            domainMap.put(auth, "{"+current+"}");
            current++;
        }

        String sub = uri.replace(auth, domainMap.get(auth));

        if(uriMap.containsKey(sub)) {
            uriMap.get(sub).add(id);
            return;
        }
        Set<String> ids = new HashSet<>();
        ids.add(id);
        uriMap.put(sub, ids);
        System.out.println(sub + " " + ids);
    }

    public boolean isJoinable(String uri, String id) {
        URI u;
        try {
            u = new URI(uri);
        } catch(URISyntaxException e) {
            return false;
        }

        String auth = uri.substring(0, uri.indexOf(u.getRawPath()));
        if(!domainMap.containsKey(auth))
            return false;
        String sub = uri.replace(auth, domainMap.get(auth));
        if(!uriMap.containsKey(sub))
            return false;

        Set<String> ids = uriMap.get(sub);
        for(String i : ids) {
            if(!i.equals(id)) return true;
        }
        return false;
    }

    public boolean hasConstituents() {
        return uriMap.size() > 0;
    }
}
