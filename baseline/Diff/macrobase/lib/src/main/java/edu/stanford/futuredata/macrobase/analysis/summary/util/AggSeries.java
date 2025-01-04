package edu.stanford.futuredata.macrobase.analysis.summary.util;

public class AggSeries {
    private double[] s;

    public AggSeries(double[] s){
        this.s = s;
    }

    public double[] sub(AggSeries b){
        double[] ret = new double[s.length];
        for(int i = 0 ; i < s.length; ++i){
            ret[i] = s[i] - b.get(i);
        }
        return ret;
    }

    public double[] add(AggSeries b){
        double[] ret = new double[s.length];
        for(int i = 0 ; i < s.length; ++i){
            ret[i] = s[i] + b.get(i);
        }
        return ret;
    }

    public double get(int index){
        return s[index];
    }

    public double norm(){
        double sum = 0;
        for (double d: s){
            sum += d*d;
        }
        return Math.sqrt(sum);
    }

    public double[] getAsDoubleArray(){
        return s;
    }
}
