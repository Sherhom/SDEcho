package edu.stanford.futuredata.macrobase.sql.myUtils;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;

// just for COUNT
public class FakeGroupBy {
    private int[][] encode;
    private double[] isOutlier;
    private int[] order;
//    private AggregationOp aggOp = SUM;

    public FakeGroupBy(int[][] encode, double[] isOutlier, int[] order){
        this.encode = encode;
        this.isOutlier = isOutlier;
        this.order = order;
    }

    public long encodeKey(int[] key){
        long eKey = 0;
        for(int k : key){
            eKey = eKey << 21;
            eKey += k;
        }
        return eKey;
    }

    public double[] diffSeries(){
        int len = 0;
        for(int it: order){
            len = len > it ? len : it;
        }
        double[] d0 = new double[len];
        for(int i = 0 ; i < len; ++i) d0[i] = 0;

        for(int i = 0 ; i < order.length; ++i){
            if(isOutlier[i] > 0){
                d0[order[i]] ++;
            }else{
                d0[order[i]] --;
            }
        }

        return d0;
    }

}
