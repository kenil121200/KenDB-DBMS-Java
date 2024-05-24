package com.kendb.query;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The `Transaction` is for processing transaction.
 */
public class Transaction {
	
	QueryChecker qc; // instance for queryChecker 
	String transactionName;
	String userName;
	FileWriter queryLogs;
	static Pattern COMMIT = Pattern.compile("commit transaction (.*);", Pattern.CASE_INSENSITIVE); // Create commit regex
    static Pattern ROLLBACK = Pattern.compile("rollback transaction (.*);", Pattern.CASE_INSENSITIVE); // Create rollback regex
    String DATABASE_ROOT_PATH = "src/resources/Database/";
    String activeDatabase = com.kendb.query.QueryChecker.activeDatabase; 
    		
    /**
     * Constructor for the Transaction class.
     *
     * @param transactionName The name of transaction.
    */
    public Transaction(String transactionName, FileWriter queryLogs) {
    	this.transactionName = transactionName;
    	this.queryLogs = queryLogs;

	}
    
    /**
     * process a queries as part of a transaction.
     *
     * @param username The username associated with the transaction.
     * @throws IOException         If an I/O error occurs.
     * @throws SyntaxErrorRaiser  If a syntax error is encountered during query execution.
     */
    public void doTransaction(String username) throws IOException, SyntaxErrorRaiser{
    	List<String> allQueryList = new ArrayList<>();
        String query;
        qc = new QueryChecker(queryLogs);
        userName = username;
        Scanner reader = new Scanner(System.in);
        
        while (reader.hasNext()) {
        	query = reader.nextLine();
        	Matcher commitMatcher = COMMIT.matcher(query);
            Matcher rollbackMatcher = ROLLBACK.matcher(query);
            if (commitMatcher.find()) {
            	commitAllQueries(allQueryList);
                return;
            }
            else if (rollbackMatcher.find()) {
            	allQueryList.clear();
                return;
            }
            else {
            	allQueryList.add(query);
            }
        }
    }
    
    /**
     * Perform execution of queries.
     *
     * @param allQueryList List of queries to be executed and committed.
     * @throws SyntaxErrorRaiser  If a syntax error is encountered during query execution.
     * @throws IOException         If an I/O error occurs.
     */
	private void performCommitAllQueries(List<String> allQueryList) throws SyntaxErrorRaiser, IOException {
		for (String q : allQueryList) {
            qc.traverseQuery(q, userName, true);
        }
	}
    
	/**
     * Execute a lock and commit a all queries in a transaction.
     *
     * @param allQueryList List of queries to be executed and committed.
     * @throws SyntaxErrorRaiser  If a syntax error is encountered during query execution.
     * @throws IOException         If an I/O error occurs.
     */
	private void commitAllQueries(List<String> allQueryList) throws SyntaxErrorRaiser, IOException{
		String databasePath = DATABASE_ROOT_PATH + activeDatabase;
		try {
			lockDatabaseFolder(databasePath);
			performCommitAllQueries(allQueryList);
        } finally {
        	unlockDatabaseFolder(databasePath);
        }	
	}
	
	/**
     * Unlock the database folder allowing other to access it.
     *
     * @param databasePath Path to the database folder.
     */
	private void unlockDatabaseFolder(String databasePath) {
		File folderLockFile = new File(databasePath, "lockfile.lock");
        if (folderLockFile.exists() && folderLockFile.delete()) {
            System.out.println("Databse lock removed");
        } else {
            System.out.println("Database lock not removed");
        }
	}
	
	/**
     * Lock the database folder to give exclusive access.
     *
     * @param databasePath Path to the database folder.
     */
	private void lockDatabaseFolder(String databasePath) {
		File folderLockFile = new File(databasePath, "lockfile.lock");
        try {
            if (folderLockFile.createNewFile()) {
                System.out.println("Database is now locked.");
            } else {
                System.out.println("Database lock exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

	
    
}
