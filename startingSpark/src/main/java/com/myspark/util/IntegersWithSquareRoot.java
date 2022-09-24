package com.myspark.util;

public class IntegersWithSquareRoot {

	private int originalNumber;
	private double squareRoot;
	
	public IntegersWithSquareRoot(int i) {
		this.originalNumber = i;
		this.squareRoot = Math.sqrt(originalNumber);
	}

}
