package water.rapids.ast.prims.search;

import water.Key;
import water.MRTask;
import water.fvec.*;
import water.rapids.Env;
import water.rapids.Val;
import water.rapids.ast.AstBuiltin;
import water.rapids.vals.ValFrame;
import water.rapids.ast.AstRoot;
import water.rapids.vals.ValRow;

public class AstWhichMax extends AstBuiltin<AstWhichMax> {

    @Override public int nargs() {
        return 4;
    }

    @Override
    public String[] args() {
        return new String[]{"frame", "na_rm", "axis"};
    }

    @Override
    public String str() {
        return "which.max";
    }

    @Override
    public String example() {
        return "(which.max frame na_rm axis)";
    }

    @Override
    public String description() {
        return "Find the index of the maximum value within a frame. If axis = 0, then the max index is found " +
                "column-wise, and the result is a frame of shape [1 x ncols], where ncols is the number of columns in " +
                "the original frame. If axis = 1, then the max index is computed row-wise, and the result is a frame of shape " +
                "[nrows x 1], where nrows is the number of rows in the original frame. Flag na_rm controls treatment of " +
                "the NA values: if it is 1, then NAs are ignored; if it is 0, then presence of NAs renders the result " +
                "in that column (row) also NA. Max index of a double / integer / binary column is a double value. " +
                "Max index of a categorical / string / uuid column is NA.";
    }

    @Override
    public Val apply(Env env, Env.StackHelp stk, AstRoot[] asts) {
        Val val1 = asts[1].exec(env);
        if (val1 instanceof ValFrame) {
            Frame fr = stk.track(val1).getFrame();
            boolean na_rm = asts[2].exec(env).getNum() == 1;
            boolean axis = asts.length == 4 && (asts[3].exec(env).getNum() == 1);
            return axis ? rowwiseWhichMax(fr, na_rm) : colwiseWhichMax(fr, na_rm);
        }
        else if (val1 instanceof ValRow) {
            // This may be called from AstApply when doing per-row computations.
            double[] row = val1.getRow();
            boolean na_rm = asts[2].exec(env).getNum() == 1;
            double maxVal = Double.NEGATIVE_INFINITY;
            double maxIndex = 0;
            for (int i = 0; i < row.length; i ++) {
                if (Double.isNaN(row[i])) {
                    if (!na_rm)
                        return new ValRow(new double[]{Double.NaN}, null);
                } else {
                    if (row[i] >= maxVal && !(row[i] ==maxVal)) {
                        maxVal = row[i];
                        maxIndex = i;
                    }
                }
            }
            return new ValRow(new double[]{maxIndex}, null);
        } else
            throw new IllegalArgumentException("Incorrect argument to (which.max): expected a frame or a row, received " + val1.getClass());
    }


    /**
     * Compute row-wise which.max by rows, and return a frame consisting of a single Vec of max indexes in each row.
     */
    private ValFrame rowwiseWhichMax(Frame fr, final boolean na_rm) {
        String[] newnames = {"which.max"};
        Key<Frame> newkey = Key.make();

        // Determine how many columns of different types we have
        int n_numeric = 0, n_time = 0;
        for (Vec vec : fr.vecs()) {
            if (vec.isNumeric()) n_numeric++;
            if (vec.isTime()) n_time++;
        }
        // Compute the type of the resulting column: if all columns are TIME then the result is also time; otherwise
        // if at least one column is numeric then the result is also numeric.
        byte resType = n_numeric > 0 ? Vec.T_NUM : Vec.T_TIME;

        // Construct the frame over which the max index should be computed
        Frame compFrame = new Frame();
        for (int i = 0; i < fr.numCols(); i++) {
            Vec vec = fr.vec(i);
            if (n_numeric > 0? vec.isNumeric() : vec.isTime())
                compFrame.add(fr.name(i), vec);
        }
        Vec anyvec = compFrame.anyVec();

        // Take into account certain corner cases
        if (anyvec == null) {
            Frame res = new Frame(newkey);
            anyvec = fr.anyVec();
            if (anyvec != null) {
                // All columns in the original frame are non-numeric -> return a vec of NAs
                res.add("which.max", anyvec.makeCon(Double.NaN));
            } // else the original frame is empty, in which case we return an empty frame too
            return new ValFrame(res);
        }
        if (!na_rm && n_numeric < fr.numCols() && n_time < fr.numCols()) {
            // If some of the columns are non-numeric and na_rm==false, then the result is a vec of NAs
            Frame res = new Frame(newkey, newnames, new Vec[]{anyvec.makeCon(Double.NaN)});
            return new ValFrame(res);
        }

        // Compute over all rows
        final int numCols = compFrame.numCols();
        Frame res = new MRTask() {
            @Override
            public void map(Chunk[] cs, NewChunk nc) {
                for (int i = 0; i < cs[0]._len; i++) {
                    int numNaColumns = 0;
                    double max = Double.NEGATIVE_INFINITY;
                    int maxIndex = 0;
                    for (int j = 0; j < numCols; j++) {
                        double val = cs[j].atd(i);
                        if (Double.isNaN(val)) {
                            numNaColumns++;
                        }
                        else if(val >=  max && !(val == max)) { //Return the first occurrence of the max index
                            max = val;
                            maxIndex = j;
                        }
                    }
                    if (na_rm ? numNaColumns < numCols : numNaColumns == 0)
                        nc.addNum(maxIndex);
                    else
                        nc.addNum(Double.NaN);
                }
            }
        }.doAll(1, resType, compFrame)
                .outputFrame(newkey, newnames, null);

        // Return the result
        return new ValFrame(res);
    }


    /**
     * Compute column-wise which.max (i.e. max index of each column), and return a frame having a single row.
     */
    private ValFrame colwiseWhichMax(Frame fr, final boolean na_rm) {
        Frame res = new Frame();

        Vec vec1 = Vec.makeCon(null, 0);
        assert vec1.length() == 1;

        for (int i = 0; i < fr.numCols(); i++) {
            Vec v = fr.vec(i);
            double max = v.max();
            boolean valid = (v.isNumeric() || v.isTime() || v.isBinary()) && v.length() > 0 && (na_rm || v.naCnt() == 0);
            MaxIndexCol maxIndexCol = new MaxIndexCol(max).doAll(new byte[]{Vec.T_NUM}, v);
            Vec newvec = vec1.makeCon(valid ? maxIndexCol._maxIndex : Double.NaN, v.isTime()? Vec.T_TIME : Vec.T_NUM);
            res.add(fr.name(i), newvec);
        }

        vec1.remove();
        return new ValFrame(res);
    }

    private static class MaxIndexCol extends MRTask<MaxIndexCol>{
        double _max;
        double _maxIndex;


        MaxIndexCol(double max) {
            _max = max;
            _maxIndex = Double.POSITIVE_INFINITY;
        }

        @Override
        public void map(Chunk c, NewChunk nc) {
            long start = c.start();
            for (int i = 0; i < c._len; ++i) {
                if (c.atd(i) >= _max) {
                    _maxIndex = start + i;
                    break;
                }
            }
        }

        @Override
        public void reduce(MaxIndexCol mic) {
            _maxIndex = Math.min(_maxIndex, mic._maxIndex); //Return the first occurrence of the max index
        }
    }
}
