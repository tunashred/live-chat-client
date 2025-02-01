import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.ahocorasick.trie.Emit;
import org.ahocorasick.trie.Trie;

public class Moderator {
    private Trie bannedWords;

    public Moderator(String file_path) {
        Trie.TrieBuilder trieBuilder = Trie.builder().ignoreCase();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file_path));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                trieBuilder.addKeyword(line.trim().toLowerCase());
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.bannedWords = trieBuilder.build();
    }

    public ProcessedMessage censor(String message) {
        StringBuilder censoredMessage = new StringBuilder(message);
        boolean isCensored = false;

        for (Emit emit : bannedWords.parseText(message)) {
            int start = emit.getStart(), end = emit.getEnd();
            String replacement = "*".repeat(emit.getKeyword().length());
            censoredMessage.replace(start, end + 1, replacement);
            isCensored = true;
        }
        return new ProcessedMessage(censoredMessage.toString(), isCensored);
    }

    public static void main(String[] args) {
        Moderator moderator = new Moderator("packs/banned.txt");
        ProcessedMessage output = moderator.censor("Ohhh, damn you, you little sh1t..");
        System.out.println(output.getProcessedMessage());
    }
}
