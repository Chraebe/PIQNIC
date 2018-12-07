package dk.aau.cs.qweb.piqnic.util;

import com.google.gson.Gson;

import java.util.*;

public class QueryResult implements Iterable<Map<String, String>> {
    private final ArrayList<String> heads = new ArrayList<>();
    private ArrayList<Map<String, String>> rows = new ArrayList<>();
    private int currentIndex = 0;

    public QueryResult(List<String> heads) {
        this.heads.addAll(heads);
    }

    public List<String> getHeads() {return heads;}
    public List<Map<String, String>> getRows() {return rows;}

    public void addResultRow(Map<String, String> row) {rows.add(row);}

    public String getValue(String key) {
        return rows.get(currentIndex).get(key);
    }

    @Override
    public Iterator<Map<String, String>> iterator() {
        Iterator<Map<String, String>> it = new Iterator<Map<String, String>>() {

            @Override
            public boolean hasNext() {
                return currentIndex < rows.size() && rows.get(currentIndex) != null;
            }

            @Override
            public Map<String, String> next() {
                return rows.get(currentIndex++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return it;
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
