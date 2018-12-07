package dk.aau.cs.qweb.piqnic.jena.bind;

import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.down.PiqnicBoundIterator;
import dk.aau.cs.qweb.piqnic.jena.down.PiqnicDownTripleBindings;
import dk.aau.cs.qweb.piqnic.jena.exceptions.QueryInterruptedException;
import dk.aau.cs.qweb.piqnic.jena.graph.PiqnicGraph;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.Pair;
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
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class QueryIterPiqnicBind extends QueryIterRepeatApply {
    private final Triple pattern;
    int count = 0;
    private QueryIterator currentStage = null;
    private volatile boolean cancelRequested = false;

    public QueryIterPiqnicBind(QueryIterator input, Triple pattern, ExecutionContext cxt) {
        super(input, cxt);
        this.pattern = pattern;
    }

    protected QueryIterator nextStage(Binding binding) {
        return null;
    }

    protected QueryIterator nextStage(PiqnicBindings bindings) {
        return new BindMapper(bindings, this.pattern, this.getExecContext());
    }

    @Override
    protected boolean hasNextBinding() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (this.isFinished()) {
            return false;
        } else {
            while(true) {
                if (this.currentStage == null) {
                    this.currentStage = this.makeNextStage();
                }

                if (this.currentStage == null) {
                    return false;
                }

                if (this.cancelRequested) {
                    performRequestCancel(this.currentStage);
                }

                if (this.currentStage.hasNext()) {
                    return true;
                }

                this.currentStage.close();
                this.currentStage = null;
            }
        }
    }

    @Override
    protected Binding moveToNextBinding() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (!this.hasNextBinding()) {
            throw new NoSuchElementException(Lib.className(this) + ".next()/finished");
        } else {
            return this.currentStage.nextBinding();
        }
    }

    private QueryIterator makeNextStage() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        ++this.count;
        if (this.getInput() == null) {
            return null;
        } else {
            PiqnicBindings bindings = new PiqnicBindings();
            for(int i = 0; i < PiqnicJenaConstants.BIND_NUM; i++) {
                if(!this.getInput().hasNext()) break;
                Binding b = this.getInput().next();
                bindings.add(b);
            }

            if(bindings.size() == 0) {
                this.getInput().close();
                return null;
            }

            return nextStage(bindings);
        }
    }

    static class BindMapper extends QueryIter {
        private Node s;
        private Node p;
        private Node o;
        private Binding slot = null;
        private boolean finished = false;
        private volatile boolean cancelled = false;
        private ExtendedIterator<Pair<Triple, Binding>> iter;

        BindMapper(PiqnicBindings bindings, Triple pattern, ExecutionContext cxt) {
            super(cxt);
            this.s = pattern.getSubject();
            this.p = pattern.getPredicate();
            this.o = pattern.getObject();

            PiqnicGraph g = (PiqnicGraph) cxt.getActiveGraph();
            iter = g.graphBaseFindBind(pattern, bindings);
        }

        private Binding mapper(Triple r, Binding b) {
            BindingMap results = BindingFactory.create(b);
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
                while(this.iter.hasNext() && this.slot == null) {
                    Pair<Triple, Binding> pair = this.iter.next();
                    Binding b = this.mapper(pair.car(), pair.cdr());
                    this.slot = b;
                }

                if (this.slot == null) {
                    this.finished = true;
                }

                return this.slot != null;
            }
        }

        protected Binding moveToNextBinding() {
            if(Thread.interrupted())
                throw new QueryInterruptedException("Interrupted.");
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
