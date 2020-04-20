package com.sparklearning.model;

public class IntegerWithSquareRoot {
    Integer num;
    Double square;
    public IntegerWithSquareRoot(int i) {
        this.num = i;
        this.square = Math.sqrt(this.num);
    }
}
