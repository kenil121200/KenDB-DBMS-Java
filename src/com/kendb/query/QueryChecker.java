package com.kendb.query;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.sql.Timestamp;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * The `QueryChecker` is for processing and validating SQL queries.
 */

public class QueryChecker {
	
	String DATABASE_ROOT_PATH = "src/resources/Database/"; // path for root database 
    public static String activeDatabase = null;
    Pattern CREATE = Pattern.compile("Create table (.*) \\(((.*) (.*)(,?)( ?))*\\);", Pattern.CASE_INSENSITIVE); // Create table regex
    Pattern DROP = Pattern.compile("Drop table (.*);", Pattern.CASE_INSENSITIVE); // drop table regex
    Pattern INSERT = Pattern.compile("Insert into (.*) \\((.*(,?)( ?)).*\\) values \\((.*(,?)( ?)).*\\);", Pattern.CASE_INSENSITIVE); // insert table regex
    Pattern SELECT_ALL = Pattern.compile("Select \\* from (.*);", Pattern.CASE_INSENSITIVE); // print table regex
    Pattern SELECT_WHERE = Pattern.compile("Select \\* from (.*) where (.*)=(.*);", Pattern.CASE_INSENSITIVE); // select specific row in table regex
    Pattern DELETE_WHERE = Pattern.compile("Delete from (.*) where (.*)=(.*);", Pattern.CASE_INSENSITIVE); // delete specific row in table regex
    Pattern UPDATE_WHERE = Pattern.compile("Update (.*) Set (.*)=(.*) where (.*)=(.*);", Pattern.CASE_INSENSITIVE); // update specific value in table regex
    static Pattern TRANSACTION = Pattern.compile("Begin transaction (.*);", Pattern.CASE_INSENSITIVE); // begin transaction regex
    
    // File writer for query logs
    FileWriter queryLogs;

    /**
     * Constructer `QueryChecker` object with the specified query log file.
     *
     * @param queryLogs The `FileWriter` to write query logs to.
     */
    public QueryChecker(FileWriter queryLogs) {
        this.queryLogs = queryLogs;
    }

    /**
     * Processes and validates the given SQL query.
     *
     * @param query          The SQL query to be processed.
     * @param Username       The username associated with the query.
     * @param transactionFlag A flag indicating whether the query is part of a transaction.
     * @throws SyntaxErrorRaiser If the query is invalid or contains syntax errors.
     * @throws IOException      If an I/O error occurs while processing the query.
     */
    public void traverseQuery(String query, String Username, boolean transactionFlag) throws SyntaxErrorRaiser, IOException {
        boolean queryInvalid = true;

        // Logs related
        Date date = new Date();
        // getTime() returns current time in milliseconds
        long time = date.getTime();
        // Passed the milliseconds to constructor of Timestamp class
        Timestamp ts = new Timestamp(time);
        
        
        // Check database folder exists or not, create it if not
        String databaseName = "MyDatabase";
        boolean databaseLockFlag = isFolderLocked(DATABASE_ROOT_PATH + databaseName);
        try {
            Path databaseFolderPath = Paths.get(DATABASE_ROOT_PATH, databaseName);
            Files.createDirectories(databaseFolderPath);
            activeDatabase = databaseName;
        } catch (IOException e) {
            System.out.println("Error");
            e.printStackTrace();
        }
        
        // check the Transaction regex
        Matcher matcher = TRANSACTION.matcher(query);
        if (matcher.find()) {
        	queryInvalid = false;
        	
        	if ((databaseLockFlag &&  transactionFlag) || (!databaseLockFlag &&  !transactionFlag)) {
        		String transactionName = matcher.group(1);
                queryLogs.append("[User: ").append(Username).append(" ]").append("[Database: ").append(activeDatabase).append(" ] [Transaction : ").append(transactionName).append(" ] [Query: ").append(query).append("] [Query Type: Valid]").append("[Timestamp: ").append(String.valueOf(ts)).append(" ]\n");
                Transaction transaction = new Transaction(transactionName, queryLogs);
                transaction.doTransaction(Username);
				
        	}
        	else {
        		System.out.println("Database Locked");
        	}
        }

        // check the create regex
        matcher = CREATE.matcher(query);
        if (matcher.find()) {
            queryInvalid = false;
            if ((databaseLockFlag &&  transactionFlag) || (!databaseLockFlag &&  !transactionFlag)) {
            	
	            String tableName = matcher.group(1);
	            String[] tableMetaData = matcher.group(2).split(",");
	            HashMap<String, String> tableColumnData = new HashMap<>();
	            List<String> primaryKeys = new ArrayList<>();
	            List<String> foreignKeys = new ArrayList<>();
	
	            for (String data : tableMetaData) {
	                String[] column = data.trim().split(" ");
	                if (column.length == 2) {
	                    tableColumnData.put(column[0], column[1]);
	                } else if (column[0].equalsIgnoreCase("PRIMARY")) {
	                	primaryKeys.add(column[2]);
	                } else if (column[0].equalsIgnoreCase("FOREIGN")) {
	                	foreignKeys.add(column[4]);
	                } else {
	                    throw new SyntaxErrorRaiser("Invalid syntax");
	                }
	            }
	
	            StringBuilder metadataLine = new StringBuilder(tableName + "(");
	            for (Map.Entry<String, String> entry : tableColumnData.entrySet()) {
	                metadataLine.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
	            }
	            if (!primaryKeys.isEmpty()) {
	                metadataLine.append("PRIMARY_KEY:");
	                for (String primaryKey : primaryKeys) {
	                    metadataLine.append(primaryKey).append(",");
	                }
	            }
	            if (!foreignKeys.isEmpty()) {
	                metadataLine.append("FOREIGN_KEY:");
	                for (String foreignKey : foreignKeys) {
	                    metadataLine.append(foreignKey).append(",");
	                }
	            }
	            metadataLine.deleteCharAt(metadataLine.length() - 1); 
	            metadataLine.append(")");
	            
	            try {
	                queryLogs.append("[User: ").append(Username).append(" ]").append("[Database: ").append(activeDatabase).append(" ] [Table: ")
	                .append(tableName).append(" ] [Query: ").append(query).append("] [Query Type: Valid]").append("[Timestamp: ").append(String.valueOf(ts)).append(" ]\n");
	                File tableFile = new File(DATABASE_ROOT_PATH + activeDatabase + "/" + tableName + ".txt");
	                if (tableFile.createNewFile()) {
	                    System.out.println("Table is created successfully : " + tableName);
	                    updateTableTxt(tableName, metadataLine.toString());
	                } else {
	                    System.out.println("Table exists");
	                }
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
            }
            else {
        		System.out.println("Database Locked");
            }
            
        }
        
        // check the drop query regex
        matcher = DROP.matcher(query);
        if (matcher.find()) {
            queryInvalid = false;
            if ((databaseLockFlag &&  transactionFlag) || (!databaseLockFlag &&  !transactionFlag)) {
            	String tableName = matcher.group(1);
                if (matcher.group(1).contains(" ")) {
                    throw new SyntaxErrorRaiser("Table name has a white space");
                }

                queryLogs.append("[User: ").append(Username).append(" ]").append("[Database: ").append(activeDatabase)
                .append(" ] [Table: ").append(tableName).append(" ] [Query: ").append(query).append("] [Query Type: Valid]")
                .append("[Timestamp: ").append(String.valueOf(ts)).append(" ]\n");
                
                File tableFile = new File(DATABASE_ROOT_PATH + activeDatabase + "/" + tableName + ".txt");
                if (tableFile.exists()) {
                    if (tableFile.delete()) {
                        System.out.println("Table named " + tableName + " is Dropped." );
                    } else {
                        System.out.println("Error in dropping table " + tableName);
                    }
                } else {
                    System.out.println("Table named " + tableName + " does not exist in the database");
                }
            }
            else {
            	System.out.println("Database Locked");
            }
            
        }
        
        // check the insert query regex
        matcher = INSERT.matcher(query);
        if (matcher.find()) {
        	queryInvalid = false;
        	if ((databaseLockFlag &&  transactionFlag) || (!databaseLockFlag &&  !transactionFlag)) {
        		String tableName = matcher.group(1);
                Path tableFile = Paths.get(DATABASE_ROOT_PATH, activeDatabase, tableName + ".txt");
                queryLogs.append("[User: ").append(Username).append(" ]").append("[Database: ").append(activeDatabase).append(" ] [Table: ").append(tableName)
                .append(" ] [Query: ").append(query).append("] [Query Type: Valid]").append("[Timestamp: ").append(String.valueOf(ts)).append(" ]\n");
                
                try {
                	List<String> tableData = Files.readAllLines(tableFile);
                    if (tableData.isEmpty()) {
                        throw new SyntaxErrorRaiser("Table does not exist.");
                    }
                    else {
                    	String[] colNames = getColNames(tableName);
                        String[] rowData = matcher.group(5).split(",");
                        HashMap<String, String> tableRowData = new HashMap<>();
                        if (colNames.length == rowData.length) {
                            for (int i = 0; i < colNames.length; i++) {
                                tableRowData.put(colNames[i].trim(), rowData[i].trim());
                            }
                        } 
                        else {
                            throw new SyntaxErrorRaiser("Invalid column names");
                        }

                        if (!tableRowData.isEmpty()) {
                            if (tableRowData.size() == colNames.length) {
                                System.out.println("Inserted 1 row into " + tableName);
                                List<String> rowsData = new ArrayList<>();
                                for (String col : colNames) {
                                    rowsData.add(tableRowData.get(col.trim()));
                                }

                                tableData.add(String.join("|", rowsData));

                                Files.write(tableFile, tableData, StandardOpenOption.TRUNCATE_EXISTING);
                            } else {
                                System.out.println("Values are missing in the query");
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
        	}
        	else {
        		System.out.println("Database Locked");
        	}
        }
        
        // check the select query regex
        matcher = SELECT_WHERE.matcher(query);
        if (matcher.find()) {
            queryInvalid = false;
            if ((databaseLockFlag &&  transactionFlag) || (!databaseLockFlag &&  !transactionFlag)) {
            	String tableName = matcher.group(1);
                String columnNameToGet = matcher.group(2);
                String columnValueToGet = matcher.group(3);
                queryLogs.append("[User: ").append(Username).append(" ]").append("[Database: ").append(activeDatabase).append(" ] [Table: ")
                .append(tableName).append(" ] [Query: ").append(query).append("] [Query Type: Valid]").append("[Timestamp: ").append(String.valueOf(ts)).append(" ]\n");
                
                try {
                    Path tableFile = Paths.get(DATABASE_ROOT_PATH, activeDatabase, tableName + ".txt");
                    List<String> tableData = Files.readAllLines(tableFile);
                    String[] colNames = getColNames(tableName);
                    int columnNameToGetIndex = -1;
                    for (int i = 0; i < colNames.length; i++) {
                        if (colNames[i].equalsIgnoreCase(columnNameToGet)) {
                        	columnNameToGetIndex = i;
                            break;
                        }
                    }

                    if (columnNameToGetIndex == -1) {
                        System.out.println("Condition column not found: " + columnNameToGet);
                    }
                    else {
                        for (String col : colNames) {
                            System.out.print(col.split(":")[0] + "\t\t");
                        }
                        System.out.println();
                        for (int i = 1; i < tableData.size(); i++) {
                            String rowFull = tableData.get(i);
                            String[] rowDataSeparate = rowFull.split("\\|");
                            if (rowDataSeparate.length > columnNameToGetIndex &&
                            		(rowDataSeparate[columnNameToGetIndex].replace("'", "")).equalsIgnoreCase(columnValueToGet)) {
                                for (String colValue : rowDataSeparate) {
                                    System.out.print(colValue + "\t\t");
                                }
                                System.out.println();
                            }
                        }
                    }
                    
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else {
        		System.out.println("Database Locked");
            }
            
            
        }
        
        // check the select all query regex
        matcher = SELECT_ALL.matcher(query);
        if (matcher.find()) {
        	queryInvalid = false;
        	
        	if ((databaseLockFlag &&  transactionFlag) || (!databaseLockFlag &&  !transactionFlag)) {
	            String tableName = matcher.group(1);
	            queryLogs.append("[User: ").append(Username).append(" ]").append("[Database: ").append(activeDatabase).append(" ] [Table: ").append(tableName).append(" ] [Query: ").append(query).append("] [Query Type: Valid]").append("[Timestamp: ").append(String.valueOf(ts)).append(" ]\n");
	            
	            try {
	                Path tableFile = Paths.get(DATABASE_ROOT_PATH, activeDatabase, tableName + ".txt");
	                List<String> tableDataFull = Files.readAllLines(tableFile);
	                
	                if (tableDataFull.isEmpty()) {
	                    System.out.println("Table " + tableName + " is empty or does not exist.");
	                }
	                else {
	                	String[] colNames = getColNames(tableName);
	                    int[] columnWidths = new int[colNames.length];
	                    for (int i = 0; i < colNames.length; i++) {
	                        columnWidths[i] = colNames[i].length();
	                    }
	                    for (int i = 1; i < tableDataFull.size(); i++) {
	                        String rowDataFull = tableDataFull.get(i);
	                        String[] rowDataSeparate = rowDataFull.split("\\|");
	                        for (int j = 0; j < rowDataSeparate.length; j++) {
	                            if (rowDataSeparate[j].length() > columnWidths[j]) {
	                                columnWidths[j] = rowDataSeparate[j].length();
	                            }
	                        }
	                    }
	                    System.out.print("+");
	                    for (int width : columnWidths) {
	                        for (int i = 0; i < width + 2; i++) {
	                            System.out.print("-");
	                        }
	                        System.out.print("+");
	                    }
	                    System.out.println();
	                    
	                    System.out.print("|");
	                    for (int i = 0; i < colNames.length; i++) {
	                        System.out.print(" " + colNames[i]);
	                        for (int j = colNames[i].length(); j < columnWidths[i]; j++) {
	                            System.out.print(" ");
	                        }
	                        System.out.print(" |");
	                    }
	                    System.out.println();
	                    
	                    System.out.print("+");
	                    for (int width : columnWidths) {
	                        for (int i = 0; i < width + 2; i++) {
	                            System.out.print("-");
	                        }
	                        System.out.print("+");
	                    }
	                    System.out.println();
	
	                    for (int i = 1; i < tableDataFull.size(); i++) {
	                        String rowData = tableDataFull.get(i);
	                        String[] rowValues = rowData.split("\\|");
	                        System.out.print("|");
	                        for (int j = 0; j < rowValues.length; j++) {
	                            System.out.print(" " + rowValues[j]);
	                            for (int k = rowValues[j].length(); k < columnWidths[j]; k++) {
	                                System.out.print(" ");
	                            }
	                            System.out.print(" |");
	                        }
	                        System.out.println();
	                    }
	                    
	                    System.out.print("+");
	                    for (int width : columnWidths) {
	                        for (int i = 0; i < width + 2; i++) {
	                            System.out.print("-");
	                        }
	                        System.out.print("+");
	                    }
	                    System.out.println();
	                }
	                
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
        	}
        	else {
        		System.out.println("Database Locked");
        	}
        }
        
        // check the Delete row query regex
        matcher = DELETE_WHERE.matcher(query);
        if (matcher.find()) {
        	queryInvalid  = false;
        	if ((databaseLockFlag &&  transactionFlag) || (!databaseLockFlag &&  !transactionFlag)) {
	            String tableName = matcher.group(1);
	            String columnNameToGet = matcher.group(2);
	            String columnValueToGet = matcher.group(3);
	            
	            queryLogs.append("[User: ").append(Username).append(" ]").append("[Database: ").append(activeDatabase).append(" ] [Table: ").append(tableName)
	            .append(" ] [Query: ").append(query).append("] [Query Type: Valid]").append("[Timestamp: ").append(String.valueOf(ts)).append(" ]\n");
	            
	            try {
	                Path tableFile = Paths.get(DATABASE_ROOT_PATH, activeDatabase, tableName + ".txt");
	                List<String> tableDataFull = Files.readAllLines(tableFile);
	
	                if (tableDataFull.isEmpty()) {
	                    System.out.println("Table " + tableName + " is empty.");
	                } else {
	                	
	                	String firstLineTable = tableDataFull.get(0);
	                    String[] colNames = getColNames(tableName);
	
	                    int columnNameToGetIndex = -1;
	                    for (int i = 0; i < colNames.length; i++) {
	                        if (colNames[i].equalsIgnoreCase(columnNameToGet)) {
	                        	columnNameToGetIndex = i;
	                            break;
	                        }
	                    }
	
	                    if (columnNameToGetIndex == -1) {
	                        System.out.println("Condition column not found: " + columnNameToGet);
	                    } else {
	                        List<String> newTableData = new ArrayList<>();
	                        newTableData.add(firstLineTable);
	
	                        int count = 0;
	
	                        for (int i = 1; i < tableDataFull.size(); i++) {
	                            String rowDataFull = tableDataFull.get(i);
	                            String[] rowDataSeparate = rowDataFull.split("\\|");
	                            if (rowDataSeparate.length > columnNameToGetIndex) {
	                                String conditionColumnValue = rowDataSeparate[columnNameToGetIndex].replace("'", "");
	                                if (conditionColumnValue.equals(columnValueToGet)) {
	                                    count++;
	                                } else {
	                                	newTableData.add(rowDataFull); 
	                                }
	                            }
	                        }
	                        Files.write(tableFile, newTableData);
	                        System.out.println("Total " + count + " row(s) are deleted in" + tableName);
	                    }
	                }
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
        	}
        	else {
        		System.out.println("Database Locked");
        	}
        }
        
        
        // check the update value row query regex
        matcher = UPDATE_WHERE.matcher(query);
        if (matcher.find()) {
        	queryInvalid = false;
        	if ((databaseLockFlag &&  transactionFlag) || (!databaseLockFlag &&  !transactionFlag)) {
        		String tableName = matcher.group(1);
                String columnToUpdate = matcher.group(2);
                String valueToUpdate = matcher.group(3);
                String columnNameToGet = matcher.group(4);
                String columnValueToGet = matcher.group(5);
                queryLogs.append("[User: ").append(Username).append(" ]").append("[Database: ").append(activeDatabase).append(" ] [Table: ").append(tableName)
                .append(" ] [Query: ").append(query).append("] [Query Type: Valid]").append("[Timestamp: ").append(String.valueOf(ts)).append(" ]\n");
                
                List<String> tableDataFull = Files.readAllLines(Paths.get(DATABASE_ROOT_PATH, activeDatabase, tableName + ".txt"));
                if (tableDataFull.isEmpty()) {
                    System.out.println("Table " + tableName + " does not exist.");
                }
                else {
                	String[] colNames = getColNames(tableName);
                	int columnIndexToUpdate = -1;
                    for (int i = 0; i < colNames.length; i++) {
                        if (colNames[i].equalsIgnoreCase(columnToUpdate)) {
                        	columnIndexToUpdate = i;
                            break;
                        }
                    }
                    
                    if (columnIndexToUpdate == -1) {
                        System.out.println("Column to update not found: " + columnToUpdate);
                        
                    }
                    
                    int columnNameToGetIndex = -1;
                    for (int i = 0; i < colNames.length; i++) {
                        if (colNames[i].equalsIgnoreCase(columnNameToGet)) {
                        	columnNameToGetIndex = i;
                            break;
                        }
                    }

                    if (columnNameToGetIndex == -1) {
                        System.out.println("Condition column not found: " + columnNameToGet);
                    }
                    
                    for (int i = 1; i < tableDataFull.size(); i++) {
                        String rowDataFull = tableDataFull.get(i);
                        String[] rowDataSeparate = rowDataFull.split("\\|");

                        if (rowDataSeparate.length > columnNameToGetIndex && 
                        		rowDataSeparate[columnNameToGetIndex].equalsIgnoreCase(columnValueToGet)) {
                        	rowDataSeparate[columnIndexToUpdate] = valueToUpdate;
                            tableDataFull.set(i, String.join("|", rowDataSeparate));
                        }
                    }
                    Files.write(Paths.get(DATABASE_ROOT_PATH, activeDatabase, tableName + ".txt"), tableDataFull);
                    System.out.println("Table data updated in " + tableName);
                }
        	}
        	else {
        		System.out.println("Database Locked");
        	}
            
        }
        if(queryInvalid) {
        	queryLogs.append("[User: ").append(Username).append(" ]").append("[Database: ").append(activeDatabase).append(" ] ").append("[Query: ").append(query).append("] [Query Type: InValid]").append("[Timestamp: ").append(String.valueOf(ts)).append(" ]\n");
            
        	throw new SyntaxErrorRaiser("Invalid Query!");
        }
    }
       
    
    /**
     * Private method to retrieve column names.
     *
     * @param tableName The name of the table for which column names are retrieved.
     * @return An array of column names.
     * @throws SyntaxErrorRaiser If the table does not exist.
     */
    private String[] getColNames(String tableName) throws SyntaxErrorRaiser {
    	
    	Path tableFilePath = Paths.get(DATABASE_ROOT_PATH, activeDatabase, tableName + ".txt");
    	String[] colNames = null;
        try {
            List<String> tableMetadata = Files.readAllLines(tableFilePath);
            if (tableMetadata.isEmpty()) {
                throw new SyntaxErrorRaiser("Table " + tableName + " does not exist.");
            }

            String header = tableMetadata.get(0);
//            System.out.println(header);
            String colNamesLine = header.split("\\(")[1];
            colNamesLine = colNamesLine.split("\\)")[0];
            String[] colNameAndType = colNamesLine.split(",");
            List<String> listColNames = new ArrayList<>();
            
            for (String cnt : colNameAndType) {
                String[] parts = cnt.split(":");

                if (parts.length >= 1) {
                    String colName = parts[0].trim();

                    if (!colName.equalsIgnoreCase("PRIMARY_KEY") && !colName.equalsIgnoreCase("FOREIGN_KEY")) {
                    	listColNames.add(colName);
                    }
                }
            }
            colNames = listColNames.toArray(new String[0]);
            
        } catch (IOException e){
            e.printStackTrace();
        }
        
		return colNames;
	}

    /**
     * Private method to update the metadata of a table.
     *
     * @param tableName     The name of the table to update.
     * @param metadataLine  The new metadata line for the table.
     */
	private void updateTableTxt(String tableName, String metadataLine) {
        try {
            FileWriter tableFileWriter = new FileWriter(DATABASE_ROOT_PATH + activeDatabase + "/" + tableName + ".txt");
            tableFileWriter.write(metadataLine); 
            tableFileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
	
	/**
	 * Private method to verify if a folder is locked by looking for a lock file.
	 *
	 * @param folderPath The path to the folder to be checked.
	 * @return `true` if the folder is locked, `false` if not.
	 */
	public static boolean isFolderLocked(String folderPath) {
        File lockFile = new File(folderPath, "lockfile.lock");
        return lockFile.exists();
    }
}
