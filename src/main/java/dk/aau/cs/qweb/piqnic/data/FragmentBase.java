package dk.aau.cs.qweb.piqnic.data;

import com.google.gson.Gson;
import dk.aau.cs.qweb.piqnic.peer.IPeer;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public abstract class FragmentBase implements IFragment {
    private final String baseUri;
    private final String id;
    private final File file;

    public FragmentBase(String baseUri, String id, File file) {
        this.baseUri = baseUri;
        this.id = id;
        this.file = file;
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



    @Override
    public String toString() {
        return toJSONString();
    }

    private String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
