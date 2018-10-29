package dk.aau.cs.qweb.pweb.util;

public class Triple {
    private String s, p, o;

    public Triple(String subject, String predicate, String object) {
        s = subject;
        p = predicate;
        o = object;
    }

    public String getSubject() {
        return s;
    }

    public void setSubject(String s) {
        this.s = s;
    }

    public String getPredicate() {
        return p;
    }

    public void setPredicate(String p) {
        this.p = p;
    }

    public String getObject() {
        return o;
    }

    public void setObject(String o) {
        this.o = o;
    }
}
