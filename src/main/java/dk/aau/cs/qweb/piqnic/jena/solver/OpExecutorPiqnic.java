package dk.aau.cs.qweb.piqnic.jena.solver;

import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.bind.QueryIterPiqnicBindBlock;
import dk.aau.cs.qweb.piqnic.jena.down.QueryIterPiqnicDownBlock;
import dk.aau.cs.qweb.piqnic.jena.graph.PiqnicGraph;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpReduced;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.util.iterator.ExtendedIterator;

public class OpExecutorPiqnic extends OpExecutor {
    private final boolean isForPiqnic;
    public final static OpExecutorFactory opExecFactoryPiqnic = new OpExecutorFactory() {
        @Override
        public OpExecutor create(ExecutionContext execCxt) {
            return new OpExecutorPiqnic(execCxt);
        }
    };

    protected OpExecutorPiqnic(ExecutionContext execCtx) {
        super(execCtx);
        isForPiqnic = execCtx.getActiveGraph() instanceof PiqnicGraph;
    }

    @Override
    protected QueryIterator execute(OpDistinct opDistinct, QueryIterator input) {
        return super.execute(opDistinct, input);
    }

    @Override
    protected QueryIterator execute(OpReduced opReduced, QueryIterator input) {
        return super.execute(opReduced, input);
    }

    @Override
    protected QueryIterator execute(OpFilter opFilter, QueryIterator input) {
        if (!isForPiqnic)
            return super.execute(opFilter, input);

        // If the filter does not apply to the input??
        // Where does ARQ catch this?

        // (filter (bgp ...))
        if (OpBGP.isBGP(opFilter.getSubOp())) {
            // Still may be a Piqnic graph in a non-piqnic dataset (e.g. a named model)
            PiqnicGraph graph = (PiqnicGraph) execCxt.getActiveGraph();
            OpBGP opBGP = (OpBGP) opFilter.getSubOp();
            return executeBGP(graph, opBGP, input, opFilter.getExprs(), execCxt);
        }

        // (filter (anything else))
        return super.execute(opFilter, input);
    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        if (!isForPiqnic)
            return super.execute(opBGP, input);

        PiqnicGraph graph = (PiqnicGraph) execCxt.getActiveGraph();
        return executeBGP(graph, opBGP, input, null, execCxt);
    }

    /**
     * Execute a BGP (and filters) on a Piqnic graph, which may be in default storage or it may be a named graph
     */
    private static QueryIterator executeBGP(PiqnicGraph graph, OpBGP opBGP, QueryIterator input, ExprList exprs,
                                            ExecutionContext execCxt) {
        // Execute a BGP on the real default graph
        return optimizeExecuteTriples(graph, input, opBGP.getPattern(), exprs, execCxt);
    }

    private static QueryIterator optimizeExecuteTriples(PiqnicGraph graph,
                                                        QueryIterator input, BasicPattern pattern, ExprList exprs,
                                                        ExecutionContext execCxt) {
        if (!input.hasNext())
            return input;

        // -- Input
        // Must pass this iterator into the next stage.
        if (pattern.size() >= 2) {
            // Must be 2 or triples to reorder.
            ReorderTransformation transform = graph.getReorderTransform();
            if (transform != null) {
                QueryIterPeek peek = QueryIterPeek.create(input, execCxt);
                input = peek; // Must pass on
                pattern = reorder(pattern, peek, transform);
            }
        }
        // -- Filter placement

        Op op = null;
        if (exprs != null)
            op = TransformFilterPlacement.transform(exprs, pattern);
        else
            op = new OpBGP(pattern);

        return plainExecute(op, input, execCxt);
    }

    /**
     * Execute without modification of the op - does <b>not</b> apply special graph name translations
     */
    private static QueryIterator plainExecute(Op op, QueryIterator input, ExecutionContext execCxt) {
        // -- Execute
        // Switch to a non-reordering executor
        // The Op may be a sequence due to TransformFilterPlacement
        // so we need to do a full execution step, not go straight to the SolverLib.

        ExecutionContext ec2 = new ExecutionContext(execCxt);
        ec2.setExecutor(plainFactory);

        // Solve without going through this executor again.
        // There would be issues of nested patterns but this is only a
        // (filter (bgp...)) or (filter (quadpattern ...)) or sequences of these.
        // so there are no nested patterns to reorder.
        return QC.execute(op, input, ec2);
    }

    private static BasicPattern reorder(BasicPattern pattern, QueryIterPeek peek, ReorderTransformation transform) {
        if (transform != null) {
            // This works by getting one result from the peek iterator,
            // and creating the more gounded BGP. The tranform is used to
            // determine the best order and the transformation is returned. This
            // transform is applied to the unsubstituted pattern (which will be
            // substituted as part of evaluation.

            if (!peek.hasNext())
                throw new ARQInternalErrorException("Peek iterator is already empty");

            BasicPattern pattern2 = Substitute.substitute(pattern, peek.peek());
            // Calculate the reordering based on the substituted pattern.
            ReorderProc proc = transform.reorderIndexes(pattern2);
            // Then reorder original patten
            pattern = proc.reorder(pattern);
        }
        return pattern;
    }

    private static OpExecutorFactory plainFactory = new OpExecutorPlainFactoryPiqnic();

    private static class OpExecutorPlainFactoryPiqnic implements OpExecutorFactory {
        @Override
        public OpExecutor create(ExecutionContext execCxt) {
            if (PiqnicJenaConstants.PROCESSOR == PiqnicJenaConstants.ProcessingType.FLOOD)
                return new OpExecutorFloodPiqnic(execCxt);
            else if (PiqnicJenaConstants.PROCESSOR == PiqnicJenaConstants.ProcessingType.BIND)
                return new OpExecutorBindPiqnic(execCxt);
            return new OpExecutorDownPiqnic(execCxt);
        }
    }

    /**
     * An op executor that simply executes a BGP or QuadPattern without any reordering
     */
    private static class OpExecutorFloodPiqnic extends OpExecutor {

        @SuppressWarnings("unchecked")
        public OpExecutorFloodPiqnic(ExecutionContext execCxt) {
            super(execCxt);
        }

        @Override
        public QueryIterator execute(OpBGP opBGP, QueryIterator input) {
            return super.execute(opBGP, input);
        }
    }

    private static class OpExecutorBindPiqnic extends OpExecutor {

        @SuppressWarnings("unchecked")
        public OpExecutorBindPiqnic(ExecutionContext execCxt) {
            super(execCxt);
        }

        @Override
        public QueryIterator execute(OpBGP opBGP, QueryIterator input) {
            return QueryIterPiqnicBindBlock.create(input, opBGP.getPattern(), execCxt);
        }
    }

    private static class OpExecutorDownPiqnic extends OpExecutor {
        @SuppressWarnings("unchecked")
        public OpExecutorDownPiqnic(ExecutionContext execCxt) {
            super(execCxt);
        }

        @Override
        public QueryIterator execute(OpBGP opBGP, QueryIterator input) {
            return QueryIterPiqnicDownBlock.create(input, opBGP.getPattern(), execCxt);
        }
    }
}
