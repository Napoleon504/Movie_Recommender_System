package com.atguigu.business.model.recom;

/**
 * Packaging of Recommended Items
 */
public class Recommendation {

    // Movie ID
    private int mid;

    // Movie Score
    private Double score;

    public Recommendation() {
    }

    public Recommendation(int mid, Double score) {
        this.mid = mid;
        this.score = score;
    }

    public int getMid() {
        return mid;
    }

    public void setMid(int mid) {
        this.mid = mid;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }
}
