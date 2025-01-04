package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

import edu.stanford.futuredata.macrobase.util.MacroBaseInternalError;

public enum AggregationOp {
    SUM, MIN, MAX, SeriesSum;

    public double combine(double a, double b) {
        switch(this) {
            case SUM: {
                return a+b;
            }
            case MIN: {
                return a < b ? a : b;
            }
            case MAX: {
                return a > b ? a : b;
            }
            default: {
                throw new MacroBaseInternalError("Invalid Aggregation Op");
            }
        }
    }

    public double[] combineDoubles(double[] a, double[] b) {
        double[] ret = new double[a.length];
        if (this == AggregationOp.SeriesSum) {
            for (int i = 0; i < a.length; ++i) {
                ret[i] = a[i] + b[i];
            }
            return ret;
        }
        throw new MacroBaseInternalError("Invalid Aggregation Op");
    }

    public double initValue() {
        switch(this) {
            case SUM: {
                return 0;
            }
            case MIN: {
                return Double.MAX_VALUE;
            }
            case MAX: {
                return -Double.MAX_VALUE;
            }
            default: {
                throw new MacroBaseInternalError("Invalid Aggregation Op");
            }
        }
    }

}
