package com.kendb.query;

// class to raise syntax error for application
public class SyntaxErrorRaiser extends Exception{
	// Constructor to create a new instance of SyntaxErrorRaiser.
	public SyntaxErrorRaiser(String error) {
		super(error);
	}
}
