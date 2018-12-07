package dk.aau.cs.qweb.piqnic.jena.down;

import dk.aau.cs.qweb.piqnic.jena.exceptions.QueryInterruptedException;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;

public class QueryIterPiqnicDown extends QueryIterRepeatApply {
    private final Triple pattern;
    private final PiqnicDownTripleBindings bindings;

    public QueryIterPiqnicDown(QueryIterator input, Triple pattern, ExecutionContext cxt, PiqnicDownTripleBindings bindings) {
        super(input, cxt);
        this.pattern = pattern;
        this.bindings = bindings;
    }

    protected QueryIterator nextStage(Binding binding) {
        return new DownMapper(binding, this.pattern, this.getExecContext(), bindings);
    }

    static class DownMapper extends QueryIter {
        private Node s;
        private Node p;
        private Node o;
        private Binding binding;
        private Binding slot = null;
        private boolean finished = false;
        private volatile boolean cancelled = false;
        private PiqnicBoundIterator bindings;

        DownMapper(Binding binding, Triple pattern, ExecutionContext cxt, PiqnicDownTripleBindings bindings) {
            super(cxt);
            this.s = substitute(pattern.getSubject(), binding);
            this.p = substitute(pattern.getPredicate(), binding);
            this.o = substitute(pattern.getObject(), binding);
            this.binding = binding;

            Node s2 = tripleNode(this.s);
            Node p2 = tripleNode(this.p);
            Node o2 = tripleNode(this.o);
            bindings.reset();
            this.bindings = new PiqnicBoundIterator(bindings, new Triple(s2,p2,o2));
        }

        private static Node tripleNode(Node node) {
            return node.isVariable() ? Node.ANY : node;
        }

        private static Node substitute(Node node, Binding binding) {
            if (Var.isVar(node)) {
                Node x = binding.get(Var.alloc(node));
                if (x != null) {
                    return x;
                }
            }

            return node;
        }

        private Binding mapper(Triple r) {
            BindingMap results = BindingFactory.create(this.binding);
            if (!insert(this.s, r.getSubject(), results)) {
                return null;
            } else if (!insert(this.p, r.getPredicate(), results)) {
                return null;
            } else {
                return !insert(this.o, r.getObject(), results) ? null : results;
            }
        }

        private static boolean insert(Node inputNode, Node outputNode, BindingMap results) {
            if (!Var.isVar(inputNode)) {
                return true;
            } else {
                Var v = Var.alloc(inputNode);
                Node x = results.get(v);
                if (x != null) {
                    return outputNode.equals(x);
                } else {
                    results.add(v, outputNode);
                    return true;
                }
            }
        }

        protected boolean hasNextBinding() {
            if(Thread.interrupted())
                throw new QueryInterruptedException("Interrupted.");

            if (this.finished) {
                return false;
            } else if (this.slot != null) {
                return true;
            } else if (this.cancelled) {
                this.finished = true;
                return false;
            } else {
                while(this.bindings.hasNext() && this.slot == null) {
                    Triple t = this.bindings.next();
                    this.slot = this.mapper(t);
                }

                if (this.slot == null) {
                    this.finished = true;
                }

                return this.slot != null;
            }
        }

        protected Binding moveToNextBinding() {
            if (!this.hasNextBinding()) {
                throw new ARQInternalErrorException();
            } else {
                Binding r = this.slot;
                this.slot = null;
                return r;
            }
        }

        protected void closeIterator() {
            /* Not Implemented. */
        }

        protected void requestCancel() {
            this.cancelled = true;
        }
    }
}
