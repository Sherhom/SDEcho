package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

import java.util.Arrays;

public class DistanceScoreMetrics implements QualityMetric{
    private double[] d0;

    @Override
    public String name() {
        return "distance_score";
    }

    @Override
    public QualityMetric initialize(double[] d0) {
        this.d0 = d0;
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        double[] d = new double[d0.length];
        for(int i = 0; i < d0.length; ++i){
            d[i] = d0[i] - aggregates[i];
        }
        return -(1+aggregates[0]/d0[0] + aggregates[1]/d0[1])*norm(Arrays.copyOfRange(d, 2, aggregates.length));
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }

    private static double norm(double[] s){
        double n = 0;
        for(double d: s){
            n += d*d;
        }
        return Math.sqrt(n);
    }
}
