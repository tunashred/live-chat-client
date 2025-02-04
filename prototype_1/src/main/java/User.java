public class User {
    private String name;
    private String userID;

    public User(String name, String userID) {
        this.name = name;
        this.userID = userID;
    }

    public User() {
    }

    public String getName() {
        return name;
    }

    public String getUserID() {
        return userID;
    }
}
