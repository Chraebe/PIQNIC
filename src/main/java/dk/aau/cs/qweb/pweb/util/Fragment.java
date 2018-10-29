package dk.aau.cs.qweb.pweb.util;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.UUID;

public class Fragment {
    private final UUID id;
    private final String predicate;
    private ArrayList<Triple> triples = new ArrayList<>();

    public Fragment(String predicate){
        id = UUID.randomUUID();
        this.predicate = predicate;
    }

    public void addTriple(Triple t) {
        triples.add(t);
    }

    public ArrayList<Triple> getTriples() {return triples;}

    public UUID getId() {
        return id;
    }

    public String getPredicate() {
        return predicate;
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
