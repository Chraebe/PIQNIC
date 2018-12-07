package dk.aau.cs.qweb.piqnic.node;

import dk.aau.cs.qweb.piqnic.config.Configuration;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;
import dk.aau.cs.qweb.piqnic.util.Triple;
import org.rdfhdt.hdt.compact.sequence.Sequence;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.*;
import org.rdfhdt.hdt.triples.impl.BitmapTriples;

import java.io.*;
import java.util.*;

public class PiqnicNode extends NodeBase {
    private Map<String, HDT> hdtMap = new HashMap<>();

    PiqnicNode(String ip, int port) {
        super(ip, port, UUID.randomUUID());
    }

    PiqnicNode(String ip, int port, UUID id) {
        super(ip, port, id);
    }

    @Override
    public void processTriplePatternBound(Triple triple, List<Map<String, String>> bindings, PrintWriter writer) {
        if (bindings.size() == 0) {
            processTriplePatternBoundNormal(triple, writer);
            return;
        }
        List<FragmentBase> fragments = new ArrayList<>();
        for (FragmentBase fragment : this.fragments) {
            if (fragment.identify(triple)) {
                //System.out.println(fragment.getBaseUri() + "/" + fragment.getId());
                fragments.add(fragment);
            }
        }

        String s = triple.getSubject();
        String p = triple.getPredicate();
        String o = triple.getObject();

        for (Map<String, String> binding : bindings) {
            String bindStr = getBindingString(binding);
            Triple t = new Triple(
                    (s.startsWith("?") && binding.containsKey(s)) ? binding.get(s) : s,
                    (p.startsWith("?") && binding.containsKey(p)) ? binding.get(p) : p,
                    (o.startsWith("?") && binding.containsKey(o)) ? binding.get(o) : o
            );
            //System.out.println(t.toString() + " " + binding + " " + bindStr);

            for (FragmentBase f : fragments) {
                processTripleBound(t, f, bindStr, writer);
            }
        }
    }

    private void processTriplePatternBoundNormal(Triple triple, PrintWriter writer) {
        for (FragmentBase fragment : fragments) {
            if (fragment.identify(triple)) {

                //System.out.println(fragment.getBaseUri() + "/" + fragment.getId() + ": " + triple.toString());
                processTriple(triple, fragment, writer);
            }
        }
    }

    private String getBindingString(Map<String, String> binding) {
        String str = "";
        Iterator<Map.Entry<String, String>> it = binding.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            str = str.concat(entry.getKey() + "=" + entry.getValue() + ";;");
        }
        if (str.length() > 2)
            str = str.substring(0, str.length() - 2);
        return str;
    }

    private void processTripleBound(Triple triple, FragmentBase fragment, String bindingString, PrintWriter writer) {
        HDT hdt;
        if (!hdtMap.containsKey(fragment.getBaseUri() + "/" + fragment.getId())) {
            try {
                hdt = HDTManager.mapIndexedHDT(fragment.getFile().getAbsolutePath());
                hdtMap.put(fragment.getBaseUri() + "/" + fragment.getId(), hdt);
            } catch (IOException e) {
                return;
            }
        } else {
            hdt = hdtMap.get(fragment.getBaseUri() + "/" + fragment.getId());
        }

        Dictionary dictionary = hdt.getDictionary();
        IteratorTripleID its;
        String s = triple.getSubject();
        String p = triple.getPredicate();
        String o = triple.getObject();

        its = hdt.getTriples().search(new TripleID((s.equals("ANY") || s.startsWith("?")) ? 0 : dictionary.stringToId(s, TripleComponentRole.SUBJECT),
                (p.equals("ANY") || p.startsWith("?")) ? 0 : dictionary.stringToId(p, TripleComponentRole.PREDICATE),
                (o.equals("ANY") || o.startsWith("?")) ? 0 : dictionary.stringToId(o, TripleComponentRole.OBJECT)));

        while (its.hasNext()) {
            TripleID ts = its.next();

            writer.println(dictionary.idToString(ts.getSubject(), TripleComponentRole.SUBJECT).toString() + ";;"
                    + dictionary.idToString(ts.getPredicate(), TripleComponentRole.PREDICATE).toString() + ";;"
                    + dictionary.idToString(ts.getObject(), TripleComponentRole.OBJECT).toString().replace("\n", " ")
                    + ";;" + bindingString);
        }
    }

    @Override
    public void processTriplePattern(Triple triple, PrintWriter writer) {
        for (FragmentBase fragment : fragments) {
            if (fragment.identify(triple)) {
                processTriple(triple, fragment, writer);
            }
        }
    }

    private void processTriple(Triple triple, FragmentBase fragment, PrintWriter writer) {
        HDT hdt;
        if (!hdtMap.containsKey(fragment.getBaseUri() + "/" + fragment.getId())) {
            try {
                hdt = HDTManager.mapIndexedHDT(fragment.getFile().getAbsolutePath());
                hdtMap.put(fragment.getBaseUri() + "/" + fragment.getId(), hdt);
            } catch (IOException e) {
                return;
            }
        } else {
            hdt = hdtMap.get(fragment.getBaseUri() + "/" + fragment.getId());
        }

        Dictionary dictionary = hdt.getDictionary();
        IteratorTripleID its;
        String s = triple.getSubject();
        String p = triple.getPredicate();
        String o = triple.getObject();

        its = hdt.getTriples().search(new TripleID((s.equals("ANY") || s.startsWith("?")) ? 0 : dictionary.stringToId(s, TripleComponentRole.SUBJECT),
                (p.equals("ANY") || p.startsWith("?")) ? 0 : dictionary.stringToId(p, TripleComponentRole.PREDICATE),
                (o.equals("ANY") || o.startsWith("?")) ? 0 : dictionary.stringToId(o, TripleComponentRole.OBJECT)));

        while (its.hasNext()) {
            TripleID ts = its.next();

            writer.println(dictionary.idToString(ts.getSubject(), TripleComponentRole.SUBJECT).toString() + ";;"
                    + dictionary.idToString(ts.getPredicate(), TripleComponentRole.PREDICATE).toString() + ";;"
                    + dictionary.idToString(ts.getObject(), TripleComponentRole.OBJECT).toString().replace("\n", " "));
        }
    }

    @Override
    public long estimateResult(Triple triple) {
        long ret = 0;
        synchronized (fragments) {
            for (FragmentBase fragment : fragments) {
                if (fragment.identify(triple)) {
                    //System.out.println(triple.toString() + " " + fragment.getId());
                    ret += estimateResultSpecific(triple, fragment);
                }
            }
        }
        return ret;
    }

    private long estimateResultSpecific(Triple triple, FragmentBase fragmentBase) {
        HDT hdt;
        if (!hdtMap.containsKey(fragmentBase.getBaseUri() + "/" + fragmentBase.getId())) {
            try {
                hdt = HDTManager.mapIndexedHDT(fragmentBase.getFile().getAbsolutePath());
                hdtMap.put(fragmentBase.getBaseUri() + "/" + fragmentBase.getId(), hdt);
            } catch (IOException e) {
                return 0L;
            }
        } else {
            hdt = hdtMap.get(fragmentBase.getBaseUri() + "/" + fragmentBase.getId());
        }
        final BitmapTriples triples = (BitmapTriples) hdt.getTriples();

        String ss = triple.getSubject();
        String sp = triple.getPredicate();
        String so = triple.getObject();

        Dictionary dictionary = hdt.getDictionary();
        int s, p, o;
        s = (ss.equals("ANY") || ss.startsWith("?")) ? 0 : dictionary.stringToId(ss, TripleComponentRole.SUBJECT);
        p = (sp.equals("ANY") || sp.startsWith("?")) ? 0 : dictionary.stringToId(sp, TripleComponentRole.PREDICATE);
        o = (so.equals("ANY") || so.startsWith("?")) ? 0 : dictionary.stringToId(so, TripleComponentRole.OBJECT);

        if (s < 0 || p < 0 || o < 0) {
            //System.out.println("Not in dataset");
            return 0L;
        }

        if (p > 0 && s == 0 && o == 0) {
            Sequence predCount = triples.getPredicateCount();
            if (predCount != null) {
                return predCount.get(p - 1);
            } else {
                // We don't know, rough estimation.
                long pred = hdt.getDictionary().getNpredicates();
                if (pred > 0) {
                    return triples.getNumberOfElements() / pred;
                } else {
                    return triples.getNumberOfElements();
                }
            }
        }

        if (s == 0 && o != 0 && triples.getIndexZ() == null) {
            return triples.getNumberOfElements();
        }

        IteratorTripleID it = triples.search(new TripleID(s, p, o));
        return it.estimatedNumResults();
    }

    @Override
    public void shuffle() throws IOException {
        //Todo Select nodes based on Relatedness
        List<IPeer> ps;
        if (neighbours.size() == 0) return;

        if (neighbours.size() <= Configuration.instance.getShuffleLength()) ps = new ArrayList<>(neighbours);
        else ps = getRandomPeers(Configuration.instance.getShuffleLength());

        neighbours.removeAll(ps);
        Random rand = new Random();
        IPeer other;
        if (ps.size() == 1) other = ps.get(0);
        else other = ps.get(rand.nextInt(ps.size()));

        ps.remove(other);
        IPeer thisp = new Peer(ip, port, id);
        ps.add(thisp);

        List<IPeer> newp = other.shuffle(ps);
        neighbours.addAll(newp);

        Set<IPeer> peerSet = new HashSet<>(neighbours);
        neighbours = new ArrayList<>(peerSet);
        neighbours.remove(new Peer(this));
        System.out.println("Shuffle complete...");
    }
}
