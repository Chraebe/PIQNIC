package dk.aau.cs.qweb.pweb.util;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

public class Dataset {
    private final UUID id;
    private final String filename;

    private ArrayList<Fragment> fragments = new ArrayList<>();

    public Dataset(String filename) {
        id = UUID.randomUUID();
        this.filename = filename;
    }

    public void addFragment(Fragment f) {fragments.add(f);}

    public Iterator<Fragment> getFragments() {return fragments.iterator();}

    public UUID getId() {
        return id;
    }

    public String getFilename() {
        return filename;
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
