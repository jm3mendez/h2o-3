package hex.glm;

import hex.DataInfo;
import hex.DataInfo.Row;

import hex.FrameTask2;
import hex.glm.GLMModel.GLMParameters;
import hex.glm.GLMModel.GLMParameters.Link;
import hex.glm.GLMModel.GLMWeightsFun;
import hex.glm.GLMModel.GLMWeights;
import hex.gram.Gram;
import hex.glm.GLMModel.GLMParameters.Family;
import water.H2O.H2OCountedCompleter;
import water.*;
import water.fvec.*;
import water.util.ArrayUtils;
import water.util.FrameUtils;
import water.util.MathUtils;
import water.util.MathUtils.BasicStats;

import java.util.Arrays;

/**
 * All GLM related distributed tasks:
 *
 * YMUTask           - computes response means on actual datasets (if some rows are ignored - e.g ignoring rows with NA and/or doing cross-validation)
 * GLMGradientTask   - computes gradient at given Beta, used by L-BFGS, for KKT condition check
 * GLMLineSearchTask - computes residual deviance(s) at given beta(s), used by line search (both L-BFGS and IRLSM)
 * GLMIterationTask  - used by IRLSM to compute Gram matrix and response t(X) W X, t(X)Wz
 *
 * @author tomasnykodym
 */
public abstract class GLMTask  {
  static class NullDevTask extends MRTask<NullDevTask> {
    double _nullDev;
    final double [] _ymu;
    final GLMWeightsFun _glmf;
    final boolean _hasWeights;
    final boolean _hasOffset;
    public NullDevTask(GLMWeightsFun glmf, double [] ymu, boolean hasWeights, boolean hasOffset) {
      _glmf = glmf;
      _ymu = ymu;
      _hasWeights = hasWeights;
      _hasOffset = hasOffset;
    }

    @Override public void map(Chunk [] chks) {
      int i = 0;
      int len = chks[0]._len;
      Chunk w = _hasWeights?chks[i++]:new C0DChunk(1.0,len);
      Chunk o = _hasOffset?chks[i++]:new C0DChunk(0.0,len);
      Chunk r = chks[i];
      if(_glmf._family != Family.multinomial) {
        double ymu = _glmf.link(_ymu[0]);
        for (int j = 0; j < len; ++j)
          _nullDev += w.atd(j)*_glmf.deviance(r.atd(j), _glmf.linkInv(ymu + o.atd(j)));
      } else {
        throw H2O.unimpl();
      }
    }
    @Override public void reduce(NullDevTask ndt) {_nullDev += ndt._nullDev;}
  }

  static class GLMResDevTask extends FrameTask2<GLMResDevTask> {
    final GLMWeightsFun _glmf;
    final double [] _beta;
    double _resDev = 0;
    long _nobs;
    double _likelihood;

    public GLMResDevTask(Key jobKey, DataInfo dinfo,GLMParameters parms, double [] beta) {
      super(null,dinfo, jobKey);
      _glmf = new GLMWeightsFun(parms);
      _beta = beta;
      _sparseOffset = _sparse?GLM.sparseOffset(_beta,_dinfo):0;
    }
    private transient GLMWeights _glmw;
    private final double _sparseOffset;

    @Override public boolean handlesSparseData(){return true;}
    @Override
    public void chunkInit() {
      _glmw = new GLMWeights();
    }
    @Override
    protected void processRow(Row r) {
      _glmf.computeWeights(r.response(0), r.innerProduct(_beta) + _sparseOffset, r.offset, r.weight, _glmw);
      _resDev += _glmw.dev;
      _likelihood += _glmw.l;
      ++_nobs;
    }
    @Override public void reduce(GLMResDevTask gt) {_nobs += gt._nobs; _resDev += gt._resDev; _likelihood += gt._likelihood;}
    public double avgDev(){return _resDev/_nobs;}
    public double dev(){return _resDev;}

  }

  static class GLMResDevTaskMultinomial extends FrameTask2<GLMResDevTaskMultinomial> {
    final double [][] _beta;
    double _likelihood;
    final int _nclasses;
    long _nobs;

    public GLMResDevTaskMultinomial(Key jobKey, DataInfo dinfo, double [] beta, int nclasses) {
      super(null,dinfo, jobKey);
      _beta = ArrayUtils.convertTo2DMatrix(beta,beta.length/nclasses);
      _nclasses = nclasses;
    }

    @Override public boolean handlesSparseData(){return true;}
    private transient double [] _sparseOffsets;

    @Override
    public void chunkInit() {
      _sparseOffsets = MemoryManager.malloc8d(_nclasses);
      if(_sparse)
        for(int c = 0; c < _nclasses; ++c)
          _sparseOffsets[c] = GLM.sparseOffset(_beta[c],_dinfo);
    }
    @Override
    protected void processRow(Row r) {
      _nobs++;
      double sumExp = 0;
      for(int c = 0; c < _nclasses; ++c)
        sumExp += Math.exp(r.innerProduct(_beta[c]) + _sparseOffsets[c]);
      int c = (int)r.response(0);
      _likelihood -= r.weight * ((r.innerProduct(_beta[c]) + _sparseOffsets[c]) - Math.log(sumExp));
    }
    @Override public void reduce(GLMResDevTaskMultinomial gt) {_nobs += gt._nobs; _likelihood += gt._likelihood;}

    public double avgDev(){return _likelihood*2/_nobs;}
    public double dev(){return _likelihood*2;}
  }

 static class WeightedSDTask extends MRTask<WeightedSDTask> {
   final int _weightId;
   final double [] _mean;
   public double [] _varSum;
   public WeightedSDTask(int wId, double [] mean){
     _weightId = wId;
     _mean = mean;
   }
   @Override public void map(Chunk [] chks){
     double [] weights = null;
     if(_weightId != - 1){
       weights = MemoryManager.malloc8d(chks[_weightId]._len);
       chks[_weightId].getDoubles(weights,0,weights.length);
       chks = ArrayUtils.remove(chks,_weightId);
     }
     _varSum = MemoryManager.malloc8d(_mean.length);
     double [] vals = MemoryManager.malloc8d(chks[0]._len);
     int [] ids = MemoryManager.malloc4(chks[0]._len);
     for(int c = 0; c < _mean.length; ++c){
       double mu = _mean[c];
       int n = chks[c].getSparseDoubles(vals,ids);
       double s = 0;
       for(int i = 0; i < n; ++i) {
         double d = vals[i];
         if(Double.isNaN(d)) // NAs are either skipped or replaced with mean (i.e. can also be skipped)
           continue;
         d = d - mu;
         if(_weightId != -1)
           s += weights[ids[i]]*d*d;
         else
           s += d*d;
       }
       _varSum[c] = s;
     }
   }
   public void reduce(WeightedSDTask t){
     ArrayUtils.add(_varSum,t._varSum);
   }
 }
 static public class YMUTask extends MRTask<YMUTask> {
   double _yMin = Double.POSITIVE_INFINITY, _yMax = Double.NEGATIVE_INFINITY;
   final int _responseId;
   final int _weightId;
   final int _offsetId;
   final int _nums; // number of numeric columns
   final int _numOff;
   final boolean _skipNAs;
   final boolean _computeWeightedMeanSigmaResponse;

   private BasicStats _basicStats;
   private BasicStats _basicStatsResponse;
   double [] _yMu;
   final int _nClasses;


   private double [] _predictorSDs;

   public double [] predictorMeans(){return _basicStats.mean();}
   public double [] predictorSDs(){
     if(_predictorSDs != null) return _predictorSDs;
     return (_predictorSDs = _basicStats.sigma());
   }

   public double [] responseMeans(){return _basicStatsResponse.mean();}
   public double [] responseSDs(){
     return _basicStatsResponse.sigma();
   }

   public YMUTask(DataInfo dinfo, int nclasses, boolean computeWeightedMeanSigmaResponse, boolean skipNAs, boolean haveResponse) {
     _nums = dinfo._nums;
     _numOff = dinfo._cats;
     _responseId = haveResponse ? dinfo.responseChunkId(0) : -1;
     _weightId = dinfo._weights?dinfo.weightChunkId():-1;
     _offsetId = dinfo._offset?dinfo.offsetChunkId():-1;
     _nClasses = nclasses;
     _computeWeightedMeanSigmaResponse = computeWeightedMeanSigmaResponse;
     _skipNAs = skipNAs;
   }

   @Override public void setupLocal(){}

   @Override public void map(Chunk [] chunks) {
     _yMu = new double[_nClasses];
     double [] ws = MemoryManager.malloc8d(chunks[0].len());
     if(_weightId != -1)
       chunks[_weightId].getDoubles(ws,0,ws.length);
     else
      Arrays.fill(ws,1);
     boolean changedWeights = false;
     if(_skipNAs) { // first find the rows to skip, need to go over all chunks including categoricals
       double [] vals = MemoryManager.malloc8d(chunks[0]._len);
       int [] ids = MemoryManager.malloc4(vals.length);
       for (int i = 0; i < chunks.length; ++i) {
         int n = vals.length;
         if(chunks[i].isSparseZero())
           n = chunks[i].getSparseDoubles(vals,ids);
         else
          chunks[i].getDoubles(vals,0,n);
         for (int r = 0; r < n; ++r) {
           if (ws[r] != 0 && Double.isNaN(vals[r])) {
             ws[r] = 0;
             changedWeights = true;
           }
         }
       }
       if(changedWeights && _weightId != -1)
         chunks[_weightId].set(ws);
     }
     Chunk response = _responseId < 0 ? null : chunks[_responseId];
     double [] numsResponse = null;
     _basicStats = new BasicStats(_nums);
     if(_computeWeightedMeanSigmaResponse) {
       _basicStatsResponse = new BasicStats(_nClasses);
       numsResponse = MemoryManager.malloc8d(_nClasses);
     }
     // compute basic stats for numeric predictors
     for(int i = 0; i < _nums; ++i) {
       Chunk c = chunks[i + _numOff];
       double w;
       for (int r = c.nextNZ(-1); r < c._len; r = c.nextNZ(r)) {
         if ((w = ws[r]) == 0) continue;
         double d = c.atd(r);
         _basicStats.add(d, w, i);
       }
     }
     if (response == null) return;
     long nobs = 0;
     double wsum = 0;
     for(double w:ws) {
       if(w != 0)++nobs;
       wsum += w;
     }
     _basicStats.setNobs(nobs,wsum);
     // compute the mean for the response
     // autoexpand categoricals into binary vecs
     for(int r = 0; r < response._len; ++r) {
       double w;
       if((w = ws[r]) == 0)
         continue;
       if(_computeWeightedMeanSigmaResponse) {
         //FIXME: Add support for subtracting offset from response
         for(int i = 0; i < _nClasses; ++i)
           numsResponse[i] = chunks[chunks.length-_nClasses+i].atd(r);
         _basicStatsResponse.add(numsResponse,w);
       }
       double d = response.atd(r);
       if(!Double.isNaN(d)) {
         if (_nClasses > 2)
           _yMu[(int) d] += w;
         else
           _yMu[0] += w*d;
         if (d < _yMin)
           _yMin = d;
         if (d > _yMax)
           _yMax = d;
       }
     }
     if(_basicStatsResponse != null)_basicStatsResponse.setNobs(nobs,wsum);
     for(int i = 0; i < _nums; ++i) {
       if(chunks[i+_numOff].isSparseZero())
         _basicStats.fillSparseZeros(i);
       else if(chunks[i+_numOff].isSparseNA())
         _basicStats.fillSparseNAs(i);
     }
   }
   @Override public void postGlobal() {
     ArrayUtils.mult(_yMu,1.0/_basicStats._wsum);
   }

   @Override public void reduce(YMUTask ymt) {
     if(ymt._basicStats.nobs() > 0 && ymt._basicStats.nobs() > 0) {
       ArrayUtils.add(_yMu,ymt._yMu);
       if(_yMin > ymt._yMin)
         _yMin = ymt._yMin;
       if(_yMax < ymt._yMax)
         _yMax = ymt._yMax;
       _basicStats.reduce(ymt._basicStats);
       if(_computeWeightedMeanSigmaResponse)
         _basicStatsResponse.reduce(ymt._basicStatsResponse);
     } else if (_basicStats.nobs() == 0) {
       _yMu = ymt._yMu;
       _yMin = ymt._yMin;
       _yMax = ymt._yMax;
       _basicStats = ymt._basicStats;
       _basicStatsResponse = ymt._basicStatsResponse;
     }
   }
   public double wsum() {return _basicStats._wsum;}

   public long nobs() {return _basicStats.nobs();}
 }

  static double  computeMultinomialEtas(double [] etas, double [] exps) {
    double maxRow = ArrayUtils.maxValue(etas);
    double sumExp = 0;
    int K = etas.length;
    for(int c = 0; c < K; ++c) {
      double x = Math.exp(etas[c] - maxRow);
      sumExp += x;
      exps[c+1] = x;
    }
    double reg = 1.0/(sumExp);
    for(int c = 0; c < etas.length; ++c)
      exps[c+1] *= reg;
    exps[0] = 0;
    exps[0] = ArrayUtils.maxIndex(exps)-1;
    return Math.log(sumExp) + maxRow;
  }



  static class GLMGenericWeightsTask extends  FrameTask2<GLMGenericWeightsTask> {

    final double [] _beta;
    double _sparseOffset;

    private final GLMWeightsFun _glmw;
    private transient GLMWeights _ws;

    double _likelihood;

    public GLMGenericWeightsTask(H2OCountedCompleter cmp, DataInfo dinfo, Key jobKey, double [] beta, GLMWeightsFun glmw) {
      super(cmp, dinfo, jobKey);
      _beta = beta;
      _glmw = glmw;
      assert _glmw._family != Family.multinomial:"Generic glm weights task does not work for family multinomial";
    }

    @Override public void chunkInit(){
      _ws = new GLMWeights();
      if(_sparse) _sparseOffset = GLM.sparseOffset(_beta,_dinfo);
    }
    @Override
    protected void processRow(Row row) {
      double eta = row.innerProduct(_beta) + _sparseOffset;
      _glmw.computeWeights(row.response(0),eta,row.offset,row.weight,_ws);
      row.setOutput(0,_ws.w);
      row.setOutput(1,_ws.z);
      _likelihood += _ws.l;
    }

    @Override public void reduce(GLMGenericWeightsTask gwt) {
      _likelihood += gwt._likelihood;
    }
  }

  static class GLMMultinomialWeightsTask extends  FrameTask2<GLMGenericWeightsTask> {

    final double [] _beta;
    double _sparseOffset;

    double _likelihood;
    final int classId;

    public GLMMultinomialWeightsTask(H2OCountedCompleter cmp, DataInfo dinfo, Key jobKey, double [] beta, int cid) {
      super(cmp, dinfo, jobKey);
      _beta = beta;
      classId = cid;
    }

    @Override public void chunkInit(){
      if(_sparse) _sparseOffset = GLM.sparseOffset(_beta,_dinfo);
    }
    @Override
    protected void processRow(Row row) {
      double y = row.response(0);
      double maxRow = row.getOutput(2);
      double etaY = row.getOutput(3);
      double eta = row.innerProduct(_beta) + _sparseOffset;
      if(classId == y){
        etaY = eta;
        row.setOutput(3,eta);
      }
      if(eta > maxRow) {
        maxRow = eta;
        row.setOutput(2,eta);
      }
      double etaExp = Math.exp(eta - maxRow);
      double sumExp = row.getOutput(4) + etaExp;
      double mu = etaExp/sumExp;
      if(mu < 1e-16) mu = 1e-16;
      double d = mu*(1-mu);
      row.setOutput(0,row.weight * d);
      row.setOutput(1,eta + (y-mu)/d); // wz = r.weight * (eta * d + (y-mu));
      _likelihood += row.weight * (etaY - Math.log(sumExp) - maxRow);
    }

    @Override public void reduce(GLMGenericWeightsTask gwt) {
      _likelihood += gwt._likelihood;
    }
  }

  static class GLMBinomialWeightsTask extends  FrameTask2<GLMGenericWeightsTask> {
    final double [] _beta;
    double _sparseOffset;
    double _likelihood;

    public GLMBinomialWeightsTask(H2OCountedCompleter cmp, DataInfo dinfo, Key jobKey, double [] beta) {
      super(cmp, dinfo, jobKey);
      _beta = beta;
    }

    @Override public void chunkInit(){
      if(_sparse) _sparseOffset = GLM.sparseOffset(_beta,_dinfo);
    }
    @Override
    protected void processRow(Row row) {
      double y = row.response(0);
      double eta = row.innerProduct(_beta) + _sparseOffset;
      double mu = 1/(Math.exp(-eta) + 1);
      if(mu < 1e-16) mu = 1e-16;
      double d = mu*(1-mu);
      row.setOutput(0,row.weight * d);
      row.setOutput(1,eta + (y-mu)/d); // wz = r.weight * (eta * d + (y-mu));
      _likelihood += row.weight * (MathUtils.y_log_y(y, mu) + MathUtils.y_log_y(1 - y, 1 - mu));
    }

    @Override public void reduce(GLMGenericWeightsTask gwt) {
      _likelihood += gwt._likelihood;
    }
  }
  static abstract class GLMGradientTask extends MRTask<GLMGradientTask> {
    final double [] _beta;
    public double [] _gradient;
    public double _likelihood;
    final transient  double _currentLambda;
    final transient double _reg;
    protected final DataInfo _dinfo;


    protected GLMGradientTask(Key jobKey, DataInfo dinfo, double reg, double lambda, double[] beta){
      _dinfo = dinfo;
      _beta = beta.clone();
      _reg = reg;
      _currentLambda = lambda;

    }
    protected abstract void computeGradientMultipliers(double [] es, double [] ys, double [] ws);

    private final void computeCategoricalEtas(Chunk [] chks, double [] etas, double [] vals, int [] ids) {
      // categoricals
      for(int cid = 0; cid < _dinfo._cats; ++cid){
        Chunk c = chks[cid];
        if(c.isSparseZero()) {
          int nvals = c.getSparseDoubles(vals,ids,_dinfo.catNAFill(cid));
          for(int i = 0; i < nvals; ++i){
            int id = _dinfo.getCategoricalId(cid,(int)vals[i]);
            if(id >=0) etas[ids[i]] += _beta[id];
          }
        } else {
          c.getIntegers(ids, 0, c._len,_dinfo.catNAFill(cid));
          for(int i = 0; i < ids.length; ++i){
            int id = _dinfo.getCategoricalId(cid,ids[i]);
            if(id >=0) etas[i] += _beta[id];
          }
        }
      }
    }

    private final void computeCategoricalGrads(Chunk [] chks, double [] etas, double [] vals, int [] ids) {
      // categoricals
      for(int cid = 0; cid < _dinfo._cats; ++cid){
        Chunk c = chks[cid];
        if(c.isSparseZero()) {
          int nvals = c.getSparseDoubles(vals,ids,_dinfo.catNAFill(cid));
          for(int i = 0; i < nvals; ++i){
            int id = _dinfo.getCategoricalId(cid,(int)vals[i]);
            if(id >=0) _gradient[id] += etas[ids[i]];
          }
        } else {
          c.getIntegers(ids, 0, c._len,_dinfo.catNAFill(cid));
          for(int i = 0; i < ids.length; ++i){
            int id = _dinfo.getCategoricalId(cid,(int)ids[i]);
            if(id >=0) _gradient[id] += etas[i];
          }
        }
      }
    }

    private final void computeNumericEtas(Chunk [] chks, double [] etas, double [] vals, int [] ids) {
      int numOff = _dinfo.numStart();
      for(int cid = 0; cid < _dinfo._nums; ++cid){
        double scale = _dinfo._normMul != null?_dinfo._normMul[cid]:1;
        double off = _dinfo._normSub != null?_dinfo._normSub[cid]:0;
        double NA = _dinfo._numMeans[cid];
        Chunk c = chks[cid+_dinfo._cats];
        double b = scale*_beta[numOff+cid];
        if(c.isSparseZero()){
          int nvals = c.getSparseDoubles(vals,ids,NA);
          for(int i = 0; i < nvals; ++i)
            etas[ids[i]] += vals[i] * b;
        } else if(c.isSparseNA()){
          int nvals = c.getSparseDoubles(vals,ids,NA);
          for(int i = 0; i < nvals; ++i)
            etas[ids[i]] += (vals[i] - off) * b;
        } else {
          c.getDoubles(vals,0,vals.length,NA);
          for(int i = 0; i < vals.length; ++i)
            etas[i] += (vals[i] - off) * b;
        }
      }
    }

    private final void computeNumericGrads(Chunk [] chks, double [] etas, double [] vals, int [] ids) {
      int numOff = _dinfo.numStart();
      for(int cid = 0; cid < _dinfo._nums; ++cid){
        double NA = _dinfo._numMeans[cid];
        Chunk c = chks[cid+_dinfo._cats];
        double scale = _dinfo._normMul == null?1:_dinfo._normMul[cid];
        if(c.isSparseZero()){
          double g = 0;
          int nVals = c.getSparseDoubles(vals,ids,NA);
          for(int i = 0; i < nVals; ++i)
            g += vals[i]*scale*etas[ids[i]];
          _gradient[numOff+cid] = g;
        } else if(c.isSparseNA()){
          double off = _dinfo._normSub == null?0:_dinfo._normSub[cid];
          double g = 0;
          int nVals = c.getSparseDoubles(vals,ids,NA);
          for(int i = 0; i < nVals; ++i)
            g += (vals[i]-off)*scale*etas[ids[i]];
          _gradient[numOff+cid] = g;
        } else {
          double off = _dinfo._normSub == null?0:_dinfo._normSub[cid];
          c.getDoubles(vals,0,vals.length,NA);
          double g = 0;
          for(int i = 0; i < vals.length; ++i)
            g += (vals[i]-off)*scale*etas[i];
          _gradient[numOff+cid] = g;
        }
      }
    }

    public void map(Chunk [] chks) {
      _gradient = MemoryManager.malloc8d(_beta.length);
      Chunk response = chks[chks.length-1];
      Chunk weights = _dinfo._weights?chks[_dinfo.weightChunkId()]:new C0DChunk(1,response._len);
      double [] ws = weights.getDoubles(MemoryManager.malloc8d(weights._len),0,weights._len);
      double [] ys = response.getDoubles(MemoryManager.malloc8d(weights._len),0,response._len);
      double [] etas = MemoryManager.malloc8d(response._len);
      if(_dinfo._offset)
        chks[_dinfo.offsetChunkId()].getDoubles(etas,0,etas.length);
      double sparseOffset = 0;
      int numStart = _dinfo.numStart();
      if(_dinfo._normSub != null)
        for(int i = 0; i < _dinfo._nums; ++i)
          if(chks[_dinfo._cats + i].isSparseZero())
            sparseOffset -= _beta[numStart + i]*_dinfo._normSub[i]*_dinfo._normMul[i];
      ArrayUtils.add(etas,sparseOffset + _beta[_beta.length-1]);
      double [] vals = MemoryManager.malloc8d(response._len);
      int [] ids = MemoryManager.malloc4(response._len);
      computeCategoricalEtas(chks,etas,vals,ids);
      computeNumericEtas(chks,etas,vals,ids);
      computeGradientMultipliers(etas,ys,ws);
      // walk the chunks again, add to the gradient
      computeCategoricalGrads(chks,etas,vals,ids);
      computeNumericGrads(chks,etas,vals,ids);
      // add intercept
      _gradient[_gradient.length-1] = ArrayUtils.sum(etas);
      if(_dinfo._normSub != null) {
        double icpt = _gradient[_gradient.length-1];
        for(int i = 0; i < _dinfo._nums; ++i) {
          if(chks[_dinfo._cats+i].isSparseZero()) {
            double d = _dinfo._normSub[i] * _dinfo._normMul[i];
            _gradient[numStart + i] -= d * icpt;
          }
        }
      }
    }

    @Override
    public final void reduce(GLMGradientTask gmgt){
      ArrayUtils.add(_gradient,gmgt._gradient);
      _likelihood += gmgt._likelihood;
    }
    @Override public final void postGlobal(){
      ArrayUtils.mult(_gradient,_reg);
      for(int j = 0; j < _beta.length - 1; ++j)
        _gradient[j] += _currentLambda * _beta[j];
    }
  }

  static class GLMGenericGradientTask extends GLMGradientTask {
    private final GLMWeightsFun _glmf;
    public GLMGenericGradientTask(Key jobKey, double obj_reg, DataInfo dinfo, GLMParameters parms, double lambda, double[] beta) {
      super(jobKey, dinfo, obj_reg, lambda, beta);
      _glmf = new GLMWeightsFun(parms);
    }

    @Override protected void computeGradientMultipliers(double [] es, double [] ys, double [] ws){
      double l = 0;
      for(int i = 0; i < es.length; ++i) {
        if (Double.isNaN(ys[i]) || ws[i] == 0) {
          es[i] = 0;
        } else {
          double mu = _glmf.linkInv(es[i]);
          l += ws[i] * _glmf.likelihood(ys[i], mu);
          double var = _glmf.variance(mu);
          if (var < 1e-6) var = 1e-6;
          es[i] = ws[i] * (mu - ys[i]) / (var * _glmf.linkDeriv(mu));
        }
      }
      _likelihood = l;
    }
  }

  static class GLMPoissonGradientTask extends GLMGradientTask {
    private final GLMWeightsFun _glmf;
    public GLMPoissonGradientTask(Key jobKey, double obj_reg, DataInfo dinfo, GLMParameters parms, double lambda, double[] beta) {
      super(jobKey, dinfo, obj_reg, lambda, beta);
      _glmf = new GLMWeightsFun(parms);
    }
    @Override protected void computeGradientMultipliers(double [] es, double [] ys, double [] ws){
      double l = 0;
      for(int i = 0; i < es.length; ++i) {
        if (Double.isNaN(ys[i]) || ws[i] == 0) {
          es[i] = 0;
        } else {
          double eta = es[i];
          double mu = Math.exp(eta);
          double yr = ys[i];
          double diff = mu - yr;
          l += ws[i] * (yr == 0?mu:yr*Math.log(yr/mu) + diff);
          es[i] = ws[i]*diff;
        }
      }
      _likelihood = 2*l;
    }
  }

  static class GLMQuasiBinomialGradientTask extends GLMGradientTask {
    private final GLMWeightsFun _glmf;
    public GLMQuasiBinomialGradientTask(Key jobKey, double obj_reg, DataInfo dinfo, GLMParameters parms, double lambda, double[] beta) {
      super(jobKey, dinfo, obj_reg, lambda, beta);
      _glmf = new GLMWeightsFun(parms);
    }
    @Override protected void computeGradientMultipliers(double [] es, double [] ys, double [] ws){
      double l = 0;
      for(int i = 0; i < es.length; ++i){
        double p = _glmf.linkInv(es[i]);
        if(p == 0) p = 1e-15;
        if(p == 1) p = 1 - 1e-15;
        es[i] = -ws[i]*(ys[i]-p);
        l += ys[i]*Math.log(p) + (1-ys[i])*Math.log(1-p);
      }
      _likelihood = -l;
    }
  }


  static class GLMBinomialGradientTask extends GLMGradientTask {
    public GLMBinomialGradientTask(Key jobKey, double obj_reg, DataInfo dinfo, GLMParameters parms, double lambda, double [] beta) {
      super(jobKey,dinfo,obj_reg,lambda,beta);
      assert parms._family == Family.binomial && parms._link == Link.logit;
    }

    @Override
    protected void computeGradientMultipliers(double[] es, double[] ys, double[] ws) {
      for(int i = 0; i < es.length; ++i) {
        if(Double.isNaN(ys[i]) || ws[i] == 0){es[i] = 0; continue;}
        double e = es[i], w = ws[i];
        double yr = ys[i];
        double ym = 1.0 / (Math.exp(-e) + 1.0);
        if(ym != yr) _likelihood += w*((MathUtils.y_log_y(yr, ym)) + MathUtils.y_log_y(1 - yr, 1 - ym));
        es[i] = ws[i] * (ym - yr);
      }
    }
  }

  static class GLMGaussianGradientTask extends GLMGradientTask {
    public GLMGaussianGradientTask(Key jobKey, double obj_reg, DataInfo dinfo, GLMParameters parms, double lambda, double [] beta) {
      super(jobKey,dinfo,obj_reg,lambda,beta);
      assert parms._family == Family.gaussian && parms._link == Link.identity;
    }

    @Override
    protected void computeGradientMultipliers(double[] es, double[] ys, double[] ws) {
      for(int i = 0; i < es.length; ++i) {
        double w = ws[i];
        if(w == 0 || Double.isNaN(ys[i])){
          es[i] = 0;
          continue;
        }
        double e = es[i], y = ys[i];
        double d = (e-y);
        double wd = w*d;
        _likelihood += wd*d;
        es[i] = wd;
      }
    }
  }

  static class GLMMultinomialGradientTask extends MRTask<GLMMultinomialGradientTask> {
    final double [][] _beta;
    final transient double _currentLambda;
    final transient double _reg;
    private double [][] _gradient;
    double _likelihood;
    Job _job;
    final boolean _sparse;
    final DataInfo _dinfo;

    /**
     *
     * @param job
     * @param dinfo
     * @param lambda
     * @param beta coefficients as 2D array [P][K]
     * @param reg
     */
    public GLMMultinomialGradientTask(Job job, DataInfo dinfo, double lambda, double[][] beta, double reg) {
      _currentLambda = lambda;
      _reg = reg;
      // need to flip the beta
      _beta = new double[beta[0].length][beta.length];
      for(int i = 0; i < _beta.length; ++i)
        for(int j = 0; j < _beta[i].length; ++j)
          _beta[i][j] = beta[j][i];
      _job = job;
      _sparse = FrameUtils.sparseRatio(dinfo._adaptedFrame) < .125;
      _dinfo = dinfo;
      if(_dinfo._offset) throw H2O.unimpl();
    }

    private final void computeCategoricalEtas(Chunk [] chks, double [][] etas, double [] vals, int [] ids) {
      // categoricals
      for(int cid = 0; cid < _dinfo._cats; ++cid){
        Chunk c = chks[cid];
        if(c.isSparseZero()) {
          int nvals = c.getSparseDoubles(vals,ids,_dinfo.catNAFill(cid));
          for(int i = 0; i < nvals; ++i){
            int id = _dinfo.getCategoricalId(cid,(int)vals[i]);
            if(id >=0)ArrayUtils.add(etas[ids[i]],_beta[id]);
          }
        } else {
          c.getIntegers(ids, 0, c._len,_dinfo.catNAFill(cid));
          for(int i = 0; i < ids.length; ++i){
            int id = _dinfo.getCategoricalId(cid,ids[i]);
            if(id >=0) ArrayUtils.add(etas[i],_beta[id]);
          }
        }
      }
    }

    private final void computeCategoricalGrads(Chunk [] chks, double [][] etas, double [] vals, int [] ids) {
      // categoricals
      for(int cid = 0; cid < _dinfo._cats; ++cid){
        Chunk c = chks[cid];
        if(c.isSparseZero()) {
          int nvals = c.getSparseDoubles(vals,ids,_dinfo.catNAFill(cid));
          for(int i = 0; i < nvals; ++i){
            int id = _dinfo.getCategoricalId(cid,(int)vals[i]);
            if(id >=0) ArrayUtils.add(_gradient[id],etas[ids[i]]);
          }
        } else {
          c.getIntegers(ids, 0, c._len,_dinfo.catNAFill(cid));
          for(int i = 0; i < ids.length; ++i){
            int id = _dinfo.getCategoricalId(cid,ids[i]);
            if(id >=0) ArrayUtils.add(_gradient[id],etas[i]);
          }
        }
      }
    }

    private final void computeNumericEtas(Chunk [] chks, double [][] etas, double [] vals, int [] ids) {
      int numOff = _dinfo.numStart();
      for(int cid = 0; cid < _dinfo._nums; ++cid){
        double [] b = _beta[numOff+cid];
        double scale = _dinfo._normMul != null?_dinfo._normMul[cid]:1;
        double NA = _dinfo._numMeans[cid];
        Chunk c = chks[cid+_dinfo._cats];
        if(c.isSparseZero() || c.isSparseNA()){
          int nvals = c.getSparseDoubles(vals,ids,NA);
          for(int i = 0; i < nvals; ++i) {
            double d = vals[i]*scale;
            ArrayUtils.wadd(etas[ids[i]],b,d);
          }
        } else {
          c.getDoubles(vals,0,vals.length,NA);
          double off = _dinfo._normSub != null?_dinfo._normSub[cid]:0;
          for(int i = 0; i < vals.length; ++i) {
            double d = (vals[i] - off) * scale;
            ArrayUtils.wadd(etas[i],b,d);
          }
        }
      }
    }

    private final void computeNumericGrads(Chunk [] chks, double [][] etas, double [] vals, int [] ids) {
      int numOff = _dinfo.numStart();
      for(int cid = 0; cid < _dinfo._nums; ++cid){
        double [] g = _gradient[numOff + cid];
        double NA = _dinfo._numMeans[cid];
        Chunk c = chks[cid+_dinfo._cats];
        double scale = _dinfo._normMul == null?1:_dinfo._normMul[cid];
        if(c.isSparseZero() || c.isSparseNA()){
          int nVals = c.getSparseDoubles(vals,ids,NA);
          for (int i = 0; i < nVals; ++i)
            ArrayUtils.wadd(g,etas[ids[i]],vals[i] * scale);
        } else {
          double off = _dinfo._normSub == null?0:_dinfo._normSub[cid];
          c.getDoubles(vals,0,vals.length,NA);
          for(int i = 0; i < vals.length; ++i)
            ArrayUtils.wadd(g,etas[i],(vals[i] - off) * scale);
        }
      }
    }

    final void computeGradientMultipliers(double [][] etas, double [] ys, double [] ws){
      int K = _beta[0].length;
      double [] exps = new double[K+1];
      for(int i = 0; i < etas.length; ++i) {
        double w = ws[i];
        if(w == 0){
          Arrays.fill(etas[i],0);
          continue;
        }
        int y = (int) ys[i];
        double logSumExp = computeMultinomialEtas(etas[i], exps);
        _likelihood -= w * (etas[i][y] - logSumExp);
        for (int c = 0; c < K; ++c)
          etas[i][c] = w * (exps[c + 1] - (y == c ? 1 : 0));
      }
    }

    @Override public void map(Chunk[] chks) {
      if(_job != null && _job.stop_requested()) throw new Job.JobCancelledException();
      int numStart = _dinfo.numStart();
      int K = _beta[0].length;// number of classes
      int P = _beta.length;   // number of predictors (+ intercept)
      int M = chks[0]._len;   // number of rows in this chunk of data
      _gradient = new double[P][K];
      double [][] etas = new double[M][K];
      double[] offsets = new double[K];
      for(int k = 0; k < K; ++k)
        offsets[k] = _beta[P-1][k]; // intercept
      // sparse offset + intercept
      if(_dinfo._normSub != null) {
        for(int i = 0; i < _dinfo._nums; ++i)
          if(chks[_dinfo._cats + i].isSparseZero())
            ArrayUtils.wadd(offsets,_beta[numStart + i], -_dinfo._normSub[i]*_dinfo._normMul[i]);
      }
      for (int i = 0; i < chks[0]._len; ++i)
        System.arraycopy(offsets, 0, etas[i], 0, K);
      Chunk response = chks[_dinfo.responseChunkId(0)];
      double [] ws = MemoryManager.malloc8d(M);
      if(_dinfo._weights) ws = chks[_dinfo.weightChunkId()].getDoubles(ws,0,M);
      else Arrays.fill(ws,1);
      chks = Arrays.copyOf(chks,chks.length-1-(_dinfo._weights?1:0));
      double [] vals = MemoryManager.malloc8d(M);
      int [] ids = MemoryManager.malloc4(M);
      computeCategoricalEtas(chks,etas,vals,ids);
      computeNumericEtas(chks,etas,vals,ids);
      computeGradientMultipliers(etas,response.getDoubles(vals,0,M),ws);
      computeCategoricalGrads(chks,etas,vals,ids);
      computeNumericGrads(chks,etas,vals,ids);
      double [] g = _gradient[P-1];
      // add intercept
      for(int i = 0; i < etas.length; ++i)
        ArrayUtils.add(g,etas[i]);
      if(_dinfo._normSub != null) {
        double [] icpt = _gradient[P-1];
        for(int i = 0; i < _dinfo._normSub.length; ++i) {
          if(chks[_dinfo._cats+i].isSparseZero())
            ArrayUtils.wadd(_gradient[numStart+i],icpt,-_dinfo._normSub[i]*_dinfo._normMul[i]);
        }
      }
    }

    @Override
    public void reduce(GLMMultinomialGradientTask gmgt){
      if(_gradient != gmgt._gradient)
        ArrayUtils.add(_gradient,gmgt._gradient);
      _likelihood += gmgt._likelihood;
    }

    @Override public void postGlobal(){
      ArrayUtils.mult(_gradient, _reg);
      int P = _beta.length;
      // add l2 penalty
      for(int c = 0; c < P-1; ++c)
        for(int j = 0; j < _beta[0].length; ++j)
          _gradient[c][j] += _currentLambda * _beta[c][j];
    }

    public double [] gradient(){
      double [] res = MemoryManager.malloc8d(_gradient.length*_gradient[0].length);
      int P = _gradient.length;
      for(int k = 0; k < _gradient[0].length; ++k)
        for(int i = 0; i < _gradient.length; ++i)
          res[k*P + i] = _gradient[i][k];
      return res;
    }
  }


//  public static class GLMCoordinateDescentTask extends MRTask<GLMCoordinateDescentTask> {
//    final double [] _betaUpdate;
//    final double [] _beta;
//    final double _xOldSub;
//    final double _xOldMul;
//    final double _xNewSub;
//    final double _xNewMul;
//
//    double [] _xy;
//
//    public GLMCoordinateDescentTask(double [] betaUpdate, double [] beta, double xOldSub, double xOldMul, double xNewSub, double xNewMul) {
//      _betaUpdate = betaUpdate;
//      _beta = beta;
//      _xOldSub = xOldSub;
//      _xOldMul = xOldMul;
//      _xNewSub = xNewSub;
//      _xNewMul = xNewMul;
//    }
//
//    public void map(Chunk [] chks) {
//      Chunk xOld = chks[0];
//      Chunk xNew = chks[1];
//      if(xNew.vec().isCategorical()){
//        _xy = MemoryManager.malloc8d(xNew.vec().domain().length);
//      } else
//      _xy = new double[1];
//      Chunk eta = chks[2];
//      Chunk weights = chks[3];
//      Chunk res = chks[4];
//      for(int i = 0; i < eta._len; ++i) {
//        double w = weights.atd(i);
//        double e = eta.atd(i);
//        if(_betaUpdate != null) {
//          if (xOld.vec().isCategorical()) {
//            int cid = (int) xOld.at8(i);
//            e = +_betaUpdate[cid];
//          } else
//            e += _betaUpdate[0] * (xOld.atd(i) - _xOldSub) * _xOldMul;
//          eta.set(i, e);
//        }
//        int cid = 0;
//        double x = w;
//        if(xNew.vec().isCategorical()) {
//          cid = (int) xNew.at8(i);
//          e -= _beta[cid];
//        } else {
//          x = (xNew.atd(i) - _xNewSub) * _xNewMul;
//          e -= _beta[0] * x;
//          x *= w;
//        }
//        _xy[cid] += x * (res.atd(i) - e);
//      }
//    }
//    @Override public void reduce(GLMCoordinateDescentTask t) {
//      ArrayUtils.add(_xy, t._xy);
//    }
//  }


//  /**
//   * Compute initial solution for multinomial problem (Simple weighted LR with all weights = 1/4)
//   */
//  public static final class GLMMultinomialInitTsk extends MRTask<GLMMultinomialInitTsk>  {
//    double [] _mu;
//    DataInfo _dinfo;
//    Gram _gram;
//    double [][] _xy;
//
//    @Override public void map(Chunk [] chks) {
//      Rows rows = _dinfo.rows(chks);
//      _gram = new Gram(_dinfo);
//      _xy = new double[_mu.length][_dinfo.fullN()+1];
//      int numStart = _dinfo.numStart();
//      double [] ds = new double[_mu.length];
//      for(int i = 0; i < ds.length; ++i)
//        ds[i] = 1.0/(_mu[i] * (1-_mu[i]));
//      for(int i = 0; i < rows._nrows; ++i) {
//        Row r = rows.row(i);
//        double y = r.response(0);
//        _gram.addRow(r,.25);
//        for(int c = 0; c < _mu.length; ++c) {
//          double iY = y == c?1:0;
//          double z = (y-_mu[c]) * ds[i];
//          for(int j = 0; j < r.nBins; ++j)
//            _xy[c][r.binIds[j]] += z;
//          for(int j = 0; j < r.nNums; ++j){
//            int id = r.numIds == null?(j + numStart):r.numIds[j];
//            double val = r.numVals[j];
//            _xy[c][id] += z*val;
//          }
//        }
//      }
//    }
//    @Override public void reduce(){
//
//    }
//  }

  /**
   * Task to compute t(X) %*% W %*%  X and t(X) %*% W %*% y
   */
  public static class LSTask extends FrameTask2<LSTask> {
    public double[] _xy;
    public Gram _gram;
    final int numStart;

    public LSTask(H2OCountedCompleter cmp, DataInfo dinfo, Key jobKey) {
      super(cmp, dinfo, jobKey);
      numStart = _dinfo.numStart();
    }

    @Override
    public void chunkInit() {
      _gram = new Gram(_dinfo.fullN(), _dinfo.largestCat(), _dinfo.numNums(), _dinfo._cats, true);
      _xy = MemoryManager.malloc8d(_dinfo.fullN() + 1);

    }

    @Override
    protected void processRow(Row r) {
      double wz = r.weight * (r.response(0) - r.offset);
      for (int i = 0; i < r.nBins; ++i) {
        _xy[r.binIds[i]] += wz;
      }
      for (int i = 0; i < r.nNums; ++i) {
        int id = r.numIds == null ? (i + numStart) : r.numIds[i];
        double val = r.numVals[i];
        _xy[id] += wz * val;
      }
      if (_dinfo._intercept)
        _xy[_xy.length - 1] += wz;
      _gram.addRow(r, r.weight);
    }

    @Override
    public void reduce(LSTask lst) {
      ArrayUtils.add(_xy, lst._xy);
      _gram.add(lst._gram);
    }

    @Override
    public void postGlobal() {
      if (_sparse && _dinfo._normSub != null) { // need to adjust gram for missing centering!
        int ns = _dinfo.numStart();
        int interceptIdx = _xy.length - 1;
        double[] interceptRow = _gram._xx[interceptIdx - _gram._diagN];
        double nobs = interceptRow[interceptRow.length - 1]; // weighted _nobs
        for (int i = ns; i < _dinfo.fullN(); ++i) {
          double iMean = _dinfo._normSub[i - ns] * _dinfo._normMul[i - ns];
          for (int j = 0; j < ns; ++j)
            _gram._xx[i - _gram._diagN][j] -= interceptRow[j] * iMean;
          for (int j = ns; j <= i; ++j) {
            double jMean = _dinfo._normSub[j - ns] * _dinfo._normMul[j - ns];
            _gram._xx[i - _gram._diagN][j] -= interceptRow[i] * jMean + interceptRow[j] * iMean - nobs * iMean * jMean;
          }
        }
        if (_dinfo._intercept) { // do the intercept row
          for (int j = ns; j < _dinfo.fullN(); ++j)
            interceptRow[j] -= nobs * _dinfo._normSub[j - ns] * _dinfo._normMul[j - ns];
        }
        // and the xy vec as well
        for (int i = ns; i < _dinfo.fullN(); ++i) {
          _xy[i] -= _xy[_xy.length - 1] * _dinfo._normSub[i - ns] * _dinfo._normMul[i - ns];
        }
      }
    }
  }

  public static class GLMWLSTask extends LSTask {
    final GLMWeightsFun _glmw;
    final double [] _beta;
    double _sparseOffset;

    public GLMWLSTask(H2OCountedCompleter cmp, DataInfo dinfo, Key jobKey, GLMWeightsFun glmw, double [] beta) {
      super(cmp, dinfo, jobKey);
      _glmw = glmw;
      _beta = beta;
    }

    private transient GLMWeights _ws;

    @Override public void chunkInit(){
      super.chunkInit();
      _ws = new GLMWeights();
    }

    @Override
    public void processRow(Row r) {
      // update weights
      double eta = r.innerProduct(_beta) + _sparseOffset;
      _glmw.computeWeights(r.response(0),eta,r.weight,r.offset,_ws);
      r.weight = _ws.w;
      r.offset = 0; // handled offset here
      r.setResponse(0,_ws.z);
      super.processRow(r);
    }
  }

  public static class GLMMultinomialWLSTask extends LSTask {
    final GLMWeightsFun _glmw;
    final double [] _beta;
    double _sparseOffset;

    public GLMMultinomialWLSTask(H2OCountedCompleter cmp, DataInfo dinfo, Key jobKey, GLMWeightsFun glmw, double [] beta) {
      super(cmp, dinfo, jobKey);
      _glmw = glmw;
      _beta = beta;
    }

    private transient GLMWeights _ws;

    @Override public void chunkInit(){
      super.chunkInit();
      _ws = new GLMWeights();
    }

    @Override
    public void processRow(Row r) {

      // update weights
      double eta = r.innerProduct(_beta) + _sparseOffset;
      _glmw.computeWeights(r.response(0),eta,r.weight,r.offset,_ws);
      r.weight = _ws.w;
      r.offset = 0; // handled offset here
      r.setResponse(0,_ws.z);
      super.processRow(r);
    }
  }


  public static class GLMIterationTaskMultinomial extends FrameTask2<GLMIterationTaskMultinomial> {
    final int _c;
    final double [] _beta; // current beta to compute update of predictors for the current class

    double [] _xy;
    Gram _gram;
    transient double _sparseOffset;

    public GLMIterationTaskMultinomial(DataInfo dinfo, Key jobKey, double [] beta, int c) {
      super(null, dinfo, jobKey);
      _beta = beta;
      _c = c;
    }

    @Override public void chunkInit(){
      // initialize
      _gram = new Gram(_dinfo.fullN(), _dinfo.largestCat(), _dinfo.numNums(), _dinfo._cats,true);
      _xy = MemoryManager.malloc8d(_dinfo.fullN()+1); // + 1 is for intercept
      if(_sparse)
        _sparseOffset = GLM.sparseOffset(_beta,_dinfo);
    }
    @Override
    protected void processRow(Row r) {
      double y = r.response(0);
      double sumExp = r.response(1);
      double maxRow = r.response(2);
      int numStart = _dinfo.numStart();
      y = (y == _c)?1:0;
      double eta = r.innerProduct(_beta) + _sparseOffset;
      if(eta > maxRow) maxRow = eta;
      double etaExp = Math.exp(eta-maxRow);
      sumExp += etaExp;
      double mu = (etaExp == Double.POSITIVE_INFINITY?1:(etaExp / sumExp));
      if(mu < 1e-16)
        mu = 1e-16;//
      double d = mu*(1-mu);
      double wz = r.weight * (eta * d + (y-mu));
      double w  = r.weight * d;
      for(int i = 0; i < r.nBins; ++i) {
        _xy[r.binIds[i]] += wz;
      }
      for(int i = 0; i < r.nNums; ++i){
        int id = r.numIds == null?(i + numStart):r.numIds[i];
        double val = r.numVals[i];
        _xy[id] += wz*val;
      }
      if(_dinfo._intercept)
        _xy[_xy.length-1] += wz;
      _gram.addRow(r, w);
    }

    @Override
    public void reduce(GLMIterationTaskMultinomial glmt) {
      ArrayUtils.add(_xy,glmt._xy);
      _gram.add(glmt._gram);
    }
  }

  public static class GLMMultinomialUpdate extends FrameTask2<GLMMultinomialUpdate> {
    private final double [][] _beta; // updated  value of beta
    private final int _c;
    private transient double [] _sparseOffsets;
    private transient double [] _etas;

    public GLMMultinomialUpdate(DataInfo dinfo, Key jobKey, double [] beta, int c) {
      super(null, dinfo, jobKey);
      _beta = ArrayUtils.convertTo2DMatrix(beta,dinfo.fullN()+1);
      _c = c;
    }

    @Override public void chunkInit(){
      // initialize
      _sparseOffsets = MemoryManager.malloc8d(_beta.length);
      _etas = MemoryManager.malloc8d(_beta.length);
      if(_sparse) {
        for(int i = 0; i < _beta.length; ++i)
          _sparseOffsets[i] = GLM.sparseOffset(_beta[i],_dinfo);
      }
    }

    private transient Chunk _sumExpChunk;
    private transient Chunk _maxRowChunk;

    @Override public void map(Chunk [] chks) {
      _sumExpChunk = chks[chks.length-2];
      _maxRowChunk = chks[chks.length-1];
      super.map(chks);
    }

    @Override
    protected void processRow(Row r) {
      double maxrow = 0;
      for(int i = 0; i < _beta.length; ++i) {
        _etas[i] = r.innerProduct(_beta[i]) + _sparseOffsets[i];
        if(_etas[i] > maxrow)
          maxrow = _etas[i];
      }
      double sumExp = 0;
      for(int i = 0; i < _beta.length; ++i)
//        if(i != _c)
          sumExp += Math.exp(_etas[i]-maxrow);
      _maxRowChunk.set(r.cid,_etas[_c]);
      _sumExpChunk.set(r.cid,Math.exp(_etas[_c]-maxrow)/sumExp);
    }
  }

  /**
   * One iteration of glm, computes weighted gram matrix and t(x)*y vector and t(y)*y scalar.
   *
   * @author tomasnykodym
   */
  public static class GLMIterationTask extends FrameTask2<GLMIterationTask> {
    final GLMWeightsFun _glmf;
    double [][]_beta_multinomial;
    double []_beta;
    protected Gram  _gram; // wx%*%x
    double [] _xy; // wx^t%*%z,
    double _yy;

    final double [] _ymu;

    long _nobs;
    public double _likelihood;
    private transient GLMWeights _w;
//    final double _lambda;
    double wsum, wsumu;
    double _sumsqe;
    int _c = -1;

    public  GLMIterationTask(Key jobKey, DataInfo dinfo, GLMWeightsFun glmw,double [] beta) {
      super(null,dinfo,jobKey);
      _beta = beta;
      _ymu = null;
      _glmf = glmw;
    }

    public  GLMIterationTask(Key jobKey, DataInfo dinfo, GLMWeightsFun glmw, double [] beta, int c) {
      super(null,dinfo,jobKey);
      _beta = beta;
      _ymu = null;
      _glmf = glmw;
      _c = c;
    }

    @Override public boolean handlesSparseData(){return true;}

    transient private double _sparseOffset;
    @Override
    public void chunkInit() {
      // initialize
      _gram = new Gram(_dinfo.fullN(), _dinfo.largestCat(), _dinfo.numNums(), _dinfo._cats,true);
      _xy = MemoryManager.malloc8d(_dinfo.fullN()+1); // + 1 is for intercept
      if(_sparse)
         _sparseOffset = GLM.sparseOffset(_beta,_dinfo);
      _w = new GLMWeights();
    }

    @Override
    protected void processRow(Row r) { // called for every row in the chunk
      if(r.isBad() || r.weight == 0) return;
      ++_nobs;
      double y = r.response(0);
      _yy += y*y;
      final int numStart = _dinfo.numStart();
      double wz,w;
      if(_glmf._family == Family.multinomial) {
        y = (y == _c)?1:0;
        double mu = r.response(1);
        double eta = r.response(2);
        double d = mu*(1-mu);
        wz = r.weight * (eta * d + (y-mu));
        w  = r.weight * d;
      } else if(_beta != null) {
        _glmf.computeWeights(y, r.innerProduct(_beta) + _sparseOffset, r.offset, r.weight, _w);
        w = _w.w;
        wz = w*_w.z;
        _likelihood += _w.l;
      } else {
        w = r.weight;
        wz = w*(y - r.offset);
      }
      wsum+=w;
      wsumu+=r.weight; // just add the user observation weight for the scaling.
      for(int i = 0; i < r.nBins; ++i)
        _xy[r.binIds[i]] += wz;
      for(int i = 0; i < r.nNums; ++i){
        int id = r.numIds == null?(i + numStart):r.numIds[i];
        double val = r.numVals[i];
        _xy[id] += wz*val;
      }
      if(_dinfo._intercept)
        _xy[_xy.length-1] += wz;
      _gram.addRow(r,w);
    }

    @Override
    public void chunkDone(){adjustForSparseStandardizedZeros();}

    @Override
    public void reduce(GLMIterationTask git){
      ArrayUtils.add(_xy, git._xy);
      _gram.add(git._gram);
      _nobs += git._nobs;
      wsum += git.wsum;
      wsumu += git.wsumu;
      _likelihood += git._likelihood;
      _sumsqe += git._sumsqe;
      _yy += git._yy;
      super.reduce(git);
    }

    private void adjustForSparseStandardizedZeros(){
      if(_sparse && _dinfo._normSub != null) { // need to adjust gram for missing centering!
        int ns = _dinfo.numStart();
        int interceptIdx = _xy.length - 1;
        double[] interceptRow = _gram._xx[interceptIdx - _gram._diagN];
        double nobs = interceptRow[interceptRow.length - 1]; // weighted _nobs
        for (int i = ns; i < _dinfo.fullN(); ++i) {
          double iMean = _dinfo._normSub[i - ns] * _dinfo._normMul[i - ns];
          for (int j = 0; j < ns; ++j)
            _gram._xx[i - _gram._diagN][j] -= interceptRow[j] * iMean;
          for (int j = ns; j <= i; ++j) {
            double jMean = _dinfo._normSub[j - ns] * _dinfo._normMul[j - ns];
            _gram._xx[i - _gram._diagN][j] -= interceptRow[i] * jMean + interceptRow[j] * iMean - nobs * iMean * jMean;
          }
        }
        if (_dinfo._intercept) { // do the intercept row
          for (int j = ns; j < _dinfo.fullN(); ++j)
            interceptRow[j] -= nobs * _dinfo._normSub[j - ns] * _dinfo._normMul[j - ns];
        }
        // and the xy vec as well
        for (int i = ns; i < _dinfo.fullN(); ++i) {
          _xy[i] -= _xy[_xy.length - 1] * _dinfo._normSub[i - ns] * _dinfo._normMul[i - ns];
        }
      }
    }

    public boolean hasNaNsOrInf() {
      return ArrayUtils.hasNaNsOrInfs(_xy) || _gram.hasNaNsOrInfs();
    }
  }

  public static class GLMCoordinateDescentTaskSeqNaiveNumNum extends MRTask<GLMCoordinateDescentTaskSeqNaiveNumNum> {
    final double _normMul;
    final double _normSub;
    final double _NA;
    final double _bOld; // current old value at j
    final double _bNew; // global beta @ j-1 that was just updated.

    double _res;
    int _iter_cnt;
    double _max; double _min;

    public GLMCoordinateDescentTaskSeqNaiveNumNum(int iter_cnt, double betaOld, double betaNew, double normMul, double normSub, double NA, double max, double min) { // pass it norm mul and norm sup - in the weights already done. norm
      _iter_cnt = iter_cnt;
      _normMul = normMul;
      _normSub = normSub;
      _bOld = betaOld;
      _bNew = betaNew;
      _NA = NA;
      _max = max;
      _min = min;
    }

    @Override
    public void map(Chunk[] chunks) {
      int cnt = 0;
      final double[] wChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] zChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] ztildaChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] xPrev = ((C8DVolatileChunk) chunks[cnt + (_iter_cnt & 1)]).getValues();
      final double[] xCurr = ((C8DVolatileChunk) chunks[cnt + 1-(_iter_cnt & 1)]).getValues();
      chunks[chunks.length - 1].getDoubles(xCurr, 0, xCurr.length, _NA, _normSub, _normMul);

      for (int i = 0; i < chunks[0]._len; ++i) { // going over all the rows in the chunk
        if(wChunk[i] == 0)continue;
        double x = xCurr[i];
        double ztilda = ztildaChunk[i] - x*_bOld + xPrev[i] * _bNew;;
        ztildaChunk[i] = ztilda; //
        _res += wChunk[i] * x * (zChunk[i] - ztilda);
      }
    }
    @Override
    public void reduce(GLMCoordinateDescentTaskSeqNaiveNumNum git){
      _res += git._res;
    }
  }

  public static class GLMCoordinateDescentTaskSeqNaiveNumIcpt extends MRTask<GLMCoordinateDescentTaskSeqNaiveNumIcpt> {
    final double _bOld; // current old value at j
    final double _bNew; // global beta @ j-1 that was just updated.
    double _res;
    int _iter_cnt;

    public GLMCoordinateDescentTaskSeqNaiveNumIcpt(int iter_cnt, double betaOld, double betaNew) { // pass it norm mul and norm sup - in the weights already done. norm
      _iter_cnt = iter_cnt;
      _bOld = betaOld;
      _bNew = betaNew;
    }
    @Override
    public void map(Chunk[] chunks) {
      int cnt = 0;
      final double[] wChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] zChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] ztildaChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] xPrev = ((C8DVolatileChunk) chunks[cnt + (_iter_cnt & 1)]).getValues();
      for (int i = 0; i < chunks[0]._len; ++i) { // going over all the rows in the chunk
        if(wChunk[i] == 0)continue;
        ztildaChunk[i] = ztildaChunk[i] - _bOld + xPrev[i] * _bNew; //
        _res += wChunk[i] * (zChunk[i] - ztildaChunk[i]);
      }
    }
    @Override
    public void reduce(GLMCoordinateDescentTaskSeqNaiveNumIcpt git){
      _res += git._res;
    }
  }
  public static class GLMCoordinateDescentTaskSeqNaiveIcptNum extends MRTask<GLMCoordinateDescentTaskSeqNaiveIcptNum> {
    final double _normMul;
    final double _normSub;
    final double _NA;
    final double _bOld; // current old value at j
    final double _bNew; // global beta @ j-1 that was just updated.
    double _res;
    int _iter_cnt;

    public GLMCoordinateDescentTaskSeqNaiveIcptNum(int iter_cnt, double betaOld, double betaNew, double normMul, double normSub, double NA) { // pass it norm mul and norm sup - in the weights already done. norm
      _iter_cnt = iter_cnt;
      _normMul = normMul;
      _normSub = normSub;
      _bOld = betaOld;
      _bNew = betaNew;
      _NA = NA;
    }

    @Override
    public void map(Chunk[] chunks) {
      int cnt = 0;
      final double[] wChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] zChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] ztildaChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();

      final double[] xCurr = ((C8DVolatileChunk) chunks[cnt + 1-(_iter_cnt & 1)]).getValues();
      chunks[chunks.length - 1].getDoubles(xCurr, 0, xCurr.length, _NA, _normSub, _normMul);
      for (int i = 0; i < chunks[0]._len; ++i) { // going over all the rows in the chunk
        if(wChunk[i] == 0)continue;
        double x = xCurr[i];
        ztildaChunk[i] = ztildaChunk[i] - x*_bOld + _bNew; //
        _res += wChunk[i] * x * (zChunk[i] - ztildaChunk[i]);
      }
    }
    @Override
    public void reduce(GLMCoordinateDescentTaskSeqNaiveIcptNum git){
      _res += git._res;
    }

  }

  public static class GLMCoordinateDescentTaskSeqNaiveIcptCat extends MRTask<GLMCoordinateDescentTaskSeqNaiveIcptCat> {
    final double [] _bOld; // current old value at j
    final double _bNew; // global beta @ j-1 that was just updated.
    final int [] _catMap;
    final int _iter_cnt;
    final int _NA;
    double []  _res;

    public GLMCoordinateDescentTaskSeqNaiveIcptCat(int iter_cnt, double [] betaOld, double betaNew, int [] catMap, int NA) { // pass it norm mul and norm sup - in the weights already done. norm
      _iter_cnt = iter_cnt;
     _bOld = betaOld;
      _bNew = betaNew;
      if(catMap != null) {
        _catMap = catMap.clone();
        for(int i = 0; i < _catMap.length; ++i)
          if(_catMap[i] == -1)
            _catMap[i] = betaOld.length-1;
      } else
        _catMap = null;
      _NA = NA;
    }
    @Override
    public void map(Chunk[] chunks) {
      _res = new double[_bOld.length];
      int cnt = 0;
      final double[] wChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] zChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] ztildaChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final int[] xCurr = ((C4VolatileChunk) chunks[cnt + 3-(_iter_cnt & 1)]).getValues();
      chunks[chunks.length - 1].getIntegers(xCurr, 0, xCurr.length, _NA);
      for (int i = 0; i < chunks[0]._len; ++i) { // going over all the rows in the chunk
        if(wChunk[i] == 0)continue;
        int cid = xCurr[i];
        if (_catMap != null)   // some levels are ignored?
          xCurr[i] = (cid = _catMap[cid]);
        ztildaChunk[i] += _bNew-_bOld[cid];
        if(cid < _res.length-1)
          _res[cid] += wChunk[i] * (zChunk[i] - ztildaChunk[i]);
      }
    }
    @Override
    public void reduce(GLMCoordinateDescentTaskSeqNaiveIcptCat git){
      ArrayUtils.add(_res,git._res);
    }
  }

  public static class GLMCoordinateDescentTaskSeqNaiveCatCat extends MRTask<GLMCoordinateDescentTaskSeqNaiveCatCat> {
    final double [] _bOld; // current old value at j
    final double [] _bNew; // global beta @ j-1 that was just updated.
    double []  _res;
    final int [] _catMap;
    final int _iter_cnt;
    final int _NA;

    public GLMCoordinateDescentTaskSeqNaiveCatCat(int iter_cnt, double [] betaOld, double [] betaNew, int [] catMap, int NA) { // pass it norm mul and norm sup - in the weights already done. norm
      _iter_cnt = iter_cnt;
      if(catMap != null) {
        _catMap = catMap.clone();
        for(int i = 0; i < _catMap.length; ++i)
          if(_catMap[i] == -1)
            _catMap[i] = betaOld.length-1;
      } else
        _catMap = null;
      _bOld = betaOld;
      _bNew = betaNew;
      _NA = NA;
    }
    @Override
    public void map(Chunk[] chunks) {
      _res = new double[_bOld.length];
      int cnt = 0;
      final double[] wChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] zChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] ztildaChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final int[] xPrev = ((C4VolatileChunk) chunks[cnt + 2 + (_iter_cnt & 1)]).getValues();
      final int[] xCurr = ((C4VolatileChunk) chunks[cnt + 3 - (_iter_cnt & 1)]).getValues();
      chunks[chunks.length - 1].getIntegers(xCurr, 0, xCurr.length, _NA);
      for (int i = 0; i < chunks[0]._len; ++i) { // going over all the rows in the chunk
        if(wChunk[i] == 0)continue;
        int cid = xCurr[i];
        if (_catMap != null)   // some levels are ignored?
          xCurr[i] = cid = _catMap[cid];
        ztildaChunk[i] = ztildaChunk[i] - _bOld[cid] + _bNew[xPrev[i]];
        if(cid < _res.length-1)
          _res[cid] += wChunk[i] * (zChunk[i] - ztildaChunk[i]);
      }
    }
    @Override
    public void reduce(GLMCoordinateDescentTaskSeqNaiveCatCat git){
      ArrayUtils.add(_res,git._res);
    }
  }

  public static class GLMCoordinateDescentTaskSeqNaiveCatNum extends MRTask<GLMCoordinateDescentTaskSeqNaiveCatNum> {
    final double _bOld; // current old value at j
    final double [] _bNew; // global beta @ j-1 that was just updated.
    double  _res;
    final double _NA;
    final int _iter_cnt;
    final double _normMul;
    final double _normSub;

    public GLMCoordinateDescentTaskSeqNaiveCatNum(int iter_cnt,double betaOld, double [] betaNew, double normMul, double normSub, double NA) { // pass it norm mul and norm sup - in the weights already done. norm
      _iter_cnt = iter_cnt;
      _bOld = betaOld;
      _bNew = betaNew;
      _NA = NA;
      _normMul = normMul;
      _normSub = normSub;
    }
    @Override
    public void map(Chunk[] chunks) {
      int cnt = 0;
      final double[] wChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] zChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] ztildaChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final int[] xPrev = ((C4VolatileChunk) chunks[cnt + 2 + (_iter_cnt & 1)]).getValues();
      final double[] xCurr = ((C8DVolatileChunk) chunks[cnt + 1-(_iter_cnt & 1)]).getValues();
      chunks[chunks.length - 1].getDoubles(xCurr, 0, xCurr.length, _NA,_normSub,_normMul);
      for (int i = 0; i < chunks[0]._len; ++i) { // going over all the rows in the chunk
        if(wChunk[i] == 0)continue;
        ztildaChunk[i] = ztildaChunk[i] - xCurr[i]*_bOld + (xPrev[i] >= 0?_bNew[xPrev[i]]:0); //
        _res += wChunk[i] * xCurr[i] *(zChunk[i] - ztildaChunk[i]);
      }
    }
    @Override
    public void reduce(GLMCoordinateDescentTaskSeqNaiveCatNum git){
      _res += git._res;
    }
  }

  public static class GLMCoordinateDescentTaskSeqNaiveCatIcpt extends MRTask<GLMCoordinateDescentTaskSeqNaiveCatIcpt> {
    final double _bOld; // current old value at j
    final double [] _bNew; // global beta @ j-1 that was just updated.
    double  _res;
    final int _iter_cnt;
    double _x2;


    public GLMCoordinateDescentTaskSeqNaiveCatIcpt(int iter_cnt,double betaOld, double [] betaNew) { // pass it norm mul and norm sup - in the weights already done. norm
      _iter_cnt = iter_cnt;
      _bOld = betaOld;
      _bNew = betaNew;
    }
    @Override
    public void map(Chunk[] chunks) {
      int cnt = 0;
      final double [] wChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double [] zChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double [] ztildaChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final int [] xPrev = ((C4VolatileChunk) chunks[cnt + 2 + (_iter_cnt & 1)]).getValues();
      for (int i = 0; i < chunks[0]._len; ++i) { // going over all the rows in the chunk
        if(wChunk[i] == 0)continue;
        double bNew = xPrev[i] >= 0?_bNew[xPrev[i]]:0;
        ztildaChunk[i] = ztildaChunk[i] - _bOld + bNew; //
        _res += wChunk[i] * (zChunk[i] - ztildaChunk[i]);
      }
    }
    @Override
    public void reduce(GLMCoordinateDescentTaskSeqNaiveCatIcpt git){
      _res += git._res;
    }
  }

  public static class GLMCoordinateDescentTaskSeqNaiveResidual extends MRTask<GLMCoordinateDescentTaskSeqNaiveResidual> {
    final double _icptDiff; // current old value at j
    double _res;

    public GLMCoordinateDescentTaskSeqNaiveResidual(double betaNew) { // pass it norm mul and norm sup - in the weights already done. norm
      _icptDiff = betaNew;
    }
    @Override
    public void map(Chunk[] chunks) {
      int cnt = 0;
      final double[] wChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] zChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      final double[] ztildaChunk = ((C8DVolatileChunk) chunks[cnt++]).getValues();
      for (int i = 0; i < chunks[0]._len; ++i) { // going over all the rows in the chunk
        if(wChunk[i] == 0)continue;
        double diff = zChunk[i] - ztildaChunk[i] - _icptDiff;
        _res += wChunk[i] * diff * diff;
      }
    }
    @Override
    public void reduce(GLMCoordinateDescentTaskSeqNaiveResidual git){
      _res += git._res;
    }
  }

  public static class GLMGenerateWeightsTask extends MRTask<GLMGenerateWeightsTask> {
    final GLMParameters _params;
    final double [] _betaw;
    double [] denums;
    double wsum,wsumu;
    DataInfo _dinfo;
    double _likelihood;
    GLMWeightsFun _glmf;

    public GLMGenerateWeightsTask(Key jobKey, DataInfo dinfo, GLMModel.GLMParameters glm, double[] betaw) {
      _params = glm;
      _betaw = betaw;
      _dinfo = dinfo;
      _glmf = new GLMWeightsFun(_params);
    }

    @Override
    public void map(Chunk [] chunks) {
      final GLMWeights glmw = new GLMWeights();
      double [] wChunk = ((C8DVolatileChunk)chunks[chunks.length-7]).getValues();
      double [] zChunk = ((C8DVolatileChunk)chunks[chunks.length-6]).getValues();
      double [] zTilda = ((C8DVolatileChunk)chunks[chunks.length-5]).getValues();
      chunks = Arrays.copyOf(chunks,chunks.length-7);
      denums = new double[_dinfo.fullN()+1]; // full N is expanded variables with categories
      Row r = _dinfo.newDenseRow();
      assert r.weight == 1 || r.weight == 0;
      for(int i = 0; i < chunks[0]._len; ++i) {
        _dinfo.extractDenseRow(chunks,i,r);
        if (r.isBad() || r.weight == 0) {
          wChunk[i] = 0;
          zChunk[i] = Double.NaN; // should not be used!
          continue;
        }
        final double y = r.response(0);
        assert ((_params._family != Family.gamma) || y > 0) : "illegal response column, y must be > 0  for family=Gamma.";
        assert ((_params._family != Family.binomial) || (0 <= y && y <= 1)) : "illegal response column, y must be <0,1>  for family=Binomial. got " + y;
        final double eta;
        final int numStart = _dinfo.numStart();
        eta = r.innerProduct(_betaw);
        _glmf.computeWeights(y,eta,0,r.weight,glmw);
        _likelihood += glmw.l;
        zTilda[i] = eta-_betaw[_betaw.length-1];
        assert glmw.w >= 0 || Double.isNaN(glmw.w) : "invalid weight " + glmw.w; // allow NaNs - can occur if line-search is needed!
        wChunk[i] = glmw.w;
        zChunk[i] = glmw.z;
        wsum+=glmw.w;
        wsumu+=r.weight; // just add the user observation weight for the scaling.
        for(int j = 0; j < r.nBins; ++j)  { // go over cat variables
          denums[r.binIds[j]] +=  glmw.w; // binIds skips the zeros.
        }
        for(int j = 0; j < r.nNums; ++j){ // num vars
          int id = r.numIds == null?(j + numStart):r.numIds[j];
          denums[id]+= glmw.w*r.get(id)*r.get(id);
        }
      }
    }

    @Override
    public void reduce(GLMGenerateWeightsTask git){ // adding contribution of all the chunks
      ArrayUtils.add(denums, git.denums);
      wsum+=git.wsum;
      wsumu += git.wsumu;
      _likelihood += git._likelihood;
      super.reduce(git);
    }


  }


  public static class ComputeSETsk extends FrameTask2<ComputeSETsk> {
//    final double [] _betaOld;
    final double [] _betaNew;
    double _sumsqe;
    double _wsum;

    public ComputeSETsk(H2OCountedCompleter cmp, DataInfo dinfo, Key jobKey, /*, double [] betaOld,*/ double [] betaNew, GLMParameters parms) {
      super(cmp, dinfo, jobKey);
//      _betaOld = betaOld;
      _glmf = new GLMWeightsFun(parms);
      _betaNew = betaNew;
    }

    transient double _sparseOffsetOld = 0;
    transient double _sparseOffsetNew = 0;
    final GLMWeightsFun _glmf;
    transient GLMWeights _glmw;
    @Override public void chunkInit(){
      if(_sparse) {
//        _sparseOffsetOld = GLM.sparseOffset(_betaNew, _dinfo);
        _sparseOffsetNew = GLM.sparseOffset(_betaNew, _dinfo);
      }
      _glmw = new GLMWeights();
    }

    @Override
    protected void processRow(Row r) {
      double z = r.response(0) - r.offset;
      double w = r.weight;
      if(_glmf._family != Family.gaussian) {
//        double etaOld = r.innerProduct(_betaOld) + _sparseOffsetOld;
        double etaOld = r.innerProduct(_betaNew) + _sparseOffsetNew;
        _glmf.computeWeights(r.response(0),etaOld,r.offset,r.weight,_glmw);
        z = _glmw.z;
        w = _glmw.w;
      }
      double eta = r.innerProduct(_betaNew) + _sparseOffsetNew;
//      double mu = _parms.linkInv(eta);
      _sumsqe += w*(eta - z)*(eta - z);
      _wsum += Math.sqrt(w);
    }
    @Override
    public void reduce(ComputeSETsk c){_sumsqe += c._sumsqe; _wsum += c._wsum;}
  }

}
