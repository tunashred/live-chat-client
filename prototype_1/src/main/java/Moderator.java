import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Moderator {
    private Set<String> bannedWords;

    public Moderator(String file_path) {
        bannedWords = new HashSet<>();
        loadBadWords(file_path);
    }

    private void loadBadWords(String file) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                bannedWords.add(line.trim().toLowerCase());
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String censor(String message) {
        String[] words = message.split(" ");
        for (int i = 0; i < words.length; i++) {
            String lowercaseWord = words[i].replaceAll("[^a-zA-Z_]", "").toLowerCase();
            if (bannedWords.contains(lowercaseWord)) {
                words[i] = "*".repeat(lowercaseWord.length());
            }
        }
        return String.join(" ", words);
    }
}
