package edu.stanford.futuredata.macrobase.sql.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

public class MinTrendDiffScoreExpression extends Node {

    private final DecimalLiteral minTrendDiffScore;

    public MinTrendDiffScoreExpression(DecimalLiteral minTrendDiffScore) {
        this(Optional.empty(), minTrendDiffScore);
    }

    public MinTrendDiffScoreExpression(NodeLocation location, DecimalLiteral minTrendDiffScore) {
        this(Optional.of(location), minTrendDiffScore);
    }

    private MinTrendDiffScoreExpression(Optional<NodeLocation> location, DecimalLiteral minTrendDiffScore) {
        super(location);
        requireNonNull(minTrendDiffScore, "minTrendDiffScore is null");
        this.minTrendDiffScore = minTrendDiffScore;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(minTrendDiffScore);
        return nodes.build();
    }

    @Override
    public int hashCode() {
        return minTrendDiffScore.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MinTrendDiffScoreExpression o = (MinTrendDiffScoreExpression) obj;
        return o.minTrendDiffScore.equals(minTrendDiffScore);
    }

    @Override
    public String toString() {
        return minTrendDiffScore.toString();
    }

    public double getMinTrendDiffScore() {
        return minTrendDiffScore.getValue();
    }
}
