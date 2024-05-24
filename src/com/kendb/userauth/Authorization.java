package com.kendb.userauth;

import java.io.*;
import java.util.Scanner;
import java.util.ArrayList;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import com.kendb.query.QueryChecker;

/**
 * This class handles authorization for user in KenDB Application.
 */

public class Authorization {
	static String LOGS_PATH = "src/resources/Logs/";
	QueryChecker queryChecker;
	
	
	/**
	 * Shows a menu for user options with KenDB.
	 *
	 * @throws Exception if an error raised during user authorization.
	 */
	public void menu() throws Exception {
		Scanner option = new Scanner(System.in);
		System.out.println("Welcome to KenDB.");
        System.out.println("========================================");
        System.out.println("Chose one option:");
        System.out.println("1. User Registration(For New User)");
        System.out.println("2. User Login(For Current User)");
        System.out.print("Please enter your choice:  ");
        int choosedOption = option.nextInt();
        switch (choosedOption) {
            case 1:
                signUp();
                break;
            case 2:
            	signIn();
                break;

        }
        option.close();
        System.out.println("========================================");
	}
	
	
	/**
	 * Handles user sign up for KenDB.
	 *
	 * @throws Exception if  error occurs during user signup.
	 */
	public void signUp() throws Exception{
		Scanner answer = new Scanner(System.in);
		System.out.println("Enter User ID and Password for KenDB Account Registration.");
		System.out.println("====================================================================");
        
		System.out.print("Enter New User ID: ");
        String newUserId = answer.nextLine();
        
        System.out.print("Enter New Password: ");
        String newPassword = answer.nextLine();
        
        String securityQuestion = null;
        System.out.println("====================================================================");
        System.out.println("Chose one of the security question:");
        System.out.println("1. Which is your favourite car?");
        System.out.println("2. What was your favorite food as a child?");
        System.out.println("3. Which is your favourite color?");
        System.out.println("4. Which is your favourite city?");
        System.out.println("====================================================================");
        System.out.print("Please enter your choice:  ");
        int choosedOption = answer.nextInt();
        answer.nextLine();
        switch (choosedOption) {
            case 1:
            	securityQuestion = "Which is your favourite car?";
                break;
            case 2:
            	securityQuestion = "What was your favorite food as a child?";
                break;
            case 3:
            	securityQuestion = "Which is your favourite color?";
                break;
            case 4:
            	securityQuestion = "Which is your favourite city?";
                break;
        }
        
        System.out.println("(Selected Security Question) " + securityQuestion);
        System.out.print("Enter your answer: ");
        String securityPassword = answer.nextLine();
        answer.close();
        
        writeUserInformation(newUserId, newPassword); // call function to store user Id and password
        writeUserSecurity(newUserId, securityQuestion, securityPassword); // call function to store user security question and answer
        System.out.println("User Created Successfully");
        
	}
	
	
	/**
	 * Handles sign-in for user in KenDB.
	 *
	 * @throws error if there is an I/O exception during sign-in.
	 */
	
	public void signIn() throws IOException {
		Scanner answer = new Scanner(System.in);
		System.out.println("Enter User ID and Password for KenDB Sign In.");
		System.out.println("====================================================================");
		answer.nextLine();
		System.out.print("Enter User ID: ");
        String userId = answer.nextLine();
        
        System.out.print("Enter Password: ");
        String password = answer.nextLine();
        
        boolean isVerified = verifySignIn(encrypt(userId), encrypt(password)); // verify the userId and password
        
        if(isVerified) {
        	ArrayList<String> security = getSecurityQuestion(encrypt(userId)); // Get user's security question and answer
        	String securityQuestion = security.get(0);
        	String securityAnswer = security.get(1);
        	System.out.println("====================================================================");
        	System.out.println(securityQuestion);
        	System.out.print("Enter Answer: ");
        	String  securityAnswerCheck = answer.nextLine();
        	if(securityAnswerCheck.equals(securityAnswer)) {
        		System.out.println("You have Successfully Logged In");
        		
        		// After login 
        		Path logsQuery = null;
                try {
                	Path folderpath = FileSystems.getDefault().getPath("src", "resources", "Logs");
					Files.createDirectories(folderpath);

					logsQuery = FileSystems.getDefault().getPath("src", "resources", "Logs", "logs_query.txt");
					if (!Files.exists(logsQuery)) {
						System.out.println("Logs file for query created. It did not exist!");
		                Files.createFile(logsQuery);
		            }

				} catch (IOException error) {
					System.out.println("Error");
		            error.printStackTrace();
				}
                
                
				FileWriter logsQueryWriter = new FileWriter(logsQuery.toFile(), true);
				queryChecker = new QueryChecker(logsQueryWriter);
				boolean isExit = true;
				while(isExit) {
					
					System.out.println("Database is  cretaed with name: MyDatabase");
					System.out.println("1. Enter the query:\n" +
	                        "2. For Exit");
					System.out.print("Enter: ");
	                String caseInput = answer.nextLine();
	                switch (caseInput) {
	                	case "1":
	                		 boolean previousMenu = false;
	                		 while(!previousMenu) {
	                			 System.out.println("For previous menu write exit ");
	                			 System.out.print("Enter the query: ");
	         			         String query  = answer.nextLine();
	         			        if(query.equalsIgnoreCase("exit")){
	         			        	previousMenu=true;
	                            }else {
		         			         try {
		         			        	 // process the query
		         			        	 queryChecker.traverseQuery(query, userId, false);
		         			         }
		         			         catch (Exception e){
		         			        	 System.out.println(e.getMessage());
		         			         }
	                            }
	                		 }
	                		 break;
	                	case "2":
	                		logsQueryWriter.close();
	                		isExit = false;
	                		break;
	                		
	                }
				}
        	}
        	else {
        		System.out.println("Two Factor Authentication Failed");
        	}
        }
        else {
        	System.out.println("User ID or Password is wrong");
        }
        answer.close();
        
	}

	/**
	 * Writes user sign up info to a file.
	 *
	 * @param newUserId    The new user's ID.
	 * @param newPassword   The new user's password.
	 */
	
	private void writeUserInformation(String newUserId, String newPassword) {
		try {
            Path folderpath = FileSystems.getDefault().getPath("src", "resources", "Users");
            Files.createDirectories(folderpath);
            FileWriter fileUserInfo = new FileWriter("src/resources/Users/user_information.txt", true);
            fileUserInfo.write(encrypt(newUserId) + "&$&" + encrypt(newPassword) + "\n");
            fileUserInfo.close();
        } catch (IOException error) {
            System.out.println("Error");
            error.printStackTrace();
        }
	}
	
	
	/**
	 * Writes user security question to a file.
	 *
	 * @param newUserId         The new user's ID.
	 * @param securityQuestion  The user's chosen security question.
	 * @param securityPassword  The user's answer to the security question.
	 */
	
	private void writeUserSecurity(String newUserId, String securityQuestion, String securityPassword) {
		try {
            FileWriter fileUserSecurity = new FileWriter("src/resources/Users/user_security.txt", true);
            fileUserSecurity.write(encrypt(newUserId) + "&$&" + securityQuestion + "&$&" + securityPassword + "\n");
            fileUserSecurity.close();
        } catch (IOException error) {
            System.out.println("Error");
            error.printStackTrace();
        }
	}
	
	/**
	 * Encrypts a password using the MD5 algorithm.
	 *
	 * @param originalString The original string to be encrypted.
	 * @return The encrypted password.
	 */
	
	private String encrypt(String originalString) {
		MessageDigest message = null;
        try {
        	message = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException error) {
            error.printStackTrace();
        }
        assert message != null;
        message.update(originalString.getBytes());
        byte[] dgst = message.digest();
        
        StringBuilder encryptString = new StringBuilder();

        for (byte byt : dgst) {
            String x = Integer.toHexString(0xFF & byt);
            if (x.length() == 1) {
            	encryptString.append('0');
            }
            encryptString.append(x);
        }

        return encryptString.toString();
	}
	
	/**
	 * Verifies the user's sign-in userId password.
	 *
	 * @param userID    The user's ID.
	 * @param password  The user's password.
	 * @return True if the password and userId are verified; false otherwise.
	 */
	
	private boolean verifySignIn(String userID, String password) {
		String userInfoPath = "src/resources/Users/user_information.txt";
		Scanner fileScanner = null;
        try {
        	fileScanner = new Scanner(new File(userInfoPath));
        } catch (FileNotFoundException error) {
        	error.printStackTrace();
        }
        ArrayList<String> lines = new ArrayList<String>();
        
		while(fileScanner.hasNextLine()) {
        	lines.add(fileScanner.nextLine());
        }
        
		int linesSize = lines.size();
		for(int index = 0; index < linesSize; index++) {
			String line = lines.get(index);
			String[] info = line.split("&\\$&");
			for(int i = 0; i < info.length; i++) {
				if(i % 1 == 0) {
					if(info[0].equals(userID) && info[1].equals(password))
						return true;
				}
			}
		}
		return false;
		
	}
	
	/**
	 * Fetch the user's security details from file.
	 *
	 * @param userID The user's ID.
	 * @return An ArrayList containing the security question and answer.
	 */
	
	private ArrayList<String> getSecurityQuestion(String userID) {
		String userSecurityPath = "src/resources/Users/user_security.txt";
		ArrayList<String> questionAnswer = new ArrayList<String>();;
		Scanner fileScanner = null;
        try {
        	fileScanner = new Scanner(new File(userSecurityPath));
        } catch (FileNotFoundException error) {
        	error.printStackTrace();
        }
        ArrayList<String> lines = new ArrayList<String>();
        
		while(fileScanner.hasNextLine()) {
        	lines.add(fileScanner.nextLine());
        }
		
		int linesSize = lines.size();
		for(int index = 0; index < linesSize; index++) {
			String line = lines.get(index);
			String[] info = line.split("&\\$&");
			if(info[0].equals(userID)) {
				questionAnswer.add(info[1]);
				questionAnswer.add(info[2]);
			}
		}
		
		return questionAnswer;
	}
}
