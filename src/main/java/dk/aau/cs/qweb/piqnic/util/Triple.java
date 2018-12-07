package dk.aau.cs.qweb.piqnic.util;

import java.util.Objects;

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

    @Override
    public String toString() {
        return "<" + s + "," + p + "," + o + ">";
    }

    @Override
    public boolean equals(Object o1) {
        if (this == o1) return true;
        if (o1 == null || getClass() != o1.getClass()) return false;
        Triple triple = (Triple) o1;
        return Objects.equals(s, triple.s) &&
                Objects.equals(p, triple.p) &&
                Objects.equals(o, triple.o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(s, p, o);
    }
}
