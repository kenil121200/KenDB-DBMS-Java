import com.kendb.userauth.Authorization;


//This be the main class for our KenDB program.
public class KenDBMain {
	// Main function to run our KenDB application.
	public static void main(String[] args) throws Exception {
		Authorization auth = new Authorization();
		auth.menu(); // Calling the menu function to show the user an options for the KenDB application.
	}

}
