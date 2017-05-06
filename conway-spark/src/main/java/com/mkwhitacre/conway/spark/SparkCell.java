package com.mkwhitacre.conway.spark;

import java.io.Serializable;

public class SparkCell implements Serializable{

    private long x;
    private long y;
    private long generation;
    private boolean isAlive;

    public SparkCell(){

    }

    public SparkCell(SparkCell cell){
        this.x = cell.getX();
        this.y = cell.getY();
        this.generation = cell.getGeneration();
        this.isAlive = cell.isAlive();
    }

    public long getX() {
        return x;
    }

    public void setX(long x) {
        this.x = x;
    }

    public long getY() {
        return y;
    }

    public void setY(long y) {
        this.y = y;
    }

    public long getGeneration() {
        return generation;
    }

    public void setGeneration(long generation) {
        this.generation = generation;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void setAlive(boolean alive) {
        isAlive = alive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SparkCell sparkCell = (SparkCell) o;

        if (x != sparkCell.x) return false;
        if (y != sparkCell.y) return false;
        if (generation != sparkCell.generation) return false;
        return isAlive == sparkCell.isAlive;

    }

    @Override
    public int hashCode() {
        int result = (int) (x ^ (x >>> 32));
        result = 31 * result + (int) (y ^ (y >>> 32));
        result = 31 * result + (int) (generation ^ (generation >>> 32));
        result = 31 * result + (isAlive ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SparkCell{" +
                "isAlive=" + isAlive +
                ", x=" + x +
                ", y=" + y +
                ", generation=" + generation +
                '}';
    }
}
