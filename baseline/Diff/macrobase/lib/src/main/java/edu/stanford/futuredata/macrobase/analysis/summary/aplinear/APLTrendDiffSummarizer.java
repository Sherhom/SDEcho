package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.*;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class APLTrendDiffSummarizer extends APLSummarizer{
    private Logger log = LoggerFactory.getLogger("APLOutlierSummarizer");
    private String countColumn = null;
    private double threshold;
    private double[] timeLine;

    public APLTrendDiffSummarizer setThreshold(double threshold){
        this.threshold = threshold;
        return this;
    }

    public APLTrendDiffSummarizer(double threshold, double[] timeLine) {
        this.threshold = threshold;
        this.timeLine = timeLine;
    }

    @Override
    public double[] getTimeLine() {
        return timeLine;
    }

    @Override
    public List<String> getAggregateNames() {
        return Arrays.asList("distance_score");
    }

    @Override
    public AggregationOp[] getAggregationOps() {
        AggregationOp[] curOps = {AggregationOp.SeriesSum};
        return curOps;
    }

    @Override
    public int[][] getEncoded(List<String[]> columns, DataFrame input) {
        return  encoder.encodeAttributesAsArray(columns);
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        double[] outlierCol = input.getDoubleColumnByName(outlierColumn);
        double[] countCol = processCountCol(input, countColumn, outlierCol.length);

        double[][] aggregateColumns = new double[2][];
        aggregateColumns[0] = outlierCol;
        aggregateColumns[1] = countCol;

        return aggregateColumns;
    }

    @Override
    public List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        qualityMetricList.add(
                new DistanceScoreMetrics()
        );
        return qualityMetricList;
    }

    @Override
    public List<Double> getThresholds() {
        return Collections.singletonList(threshold);
    }

    @Override
    public double getNumberOutliers(double[][] aggregates) {
        double count = 0.0;
        double[] outlierCount = aggregates[0];
        for (double v : outlierCount) {
            count += v;
        }
        return count;
    }

    public String getCountColumn() {
        return countColumn;
    }

    public void setCountColumn(String countColumn) {
        this.countColumn = countColumn;
    }

}
