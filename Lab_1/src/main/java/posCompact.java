import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class posCompact
{
    // This code takes the results of the tokenizatin, lemmatization, and parts of speech, and tallies the results
    // it also prints out the unique nouns and verbs in the abstract
    public static void main(String args[])
    {
        // loop through the ten papers
        for(int num = 1; num < 12; num++)
        {
            // ignores the number 10 paper as it had no abstract, and use the the number 11 paper as number 10
            if(num != 10)
            {
                Set<String> nouns = new HashSet<>();
                Set<String> verbs = new HashSet<>();
                int nounCount = 0;
                int verbCount = 0;

                // reads the file and finds the nouns and verbs
                try
                {
                    FileInputStream file = new FileInputStream("data/token_results/" + num + "_token.txt");
                    BufferedReader buff = new BufferedReader(new InputStreamReader(file));

                    String text = "";

                    while ((text = buff.readLine()) != null)
                    {
                        String[] data = text.split(" ");
                        if(data[2].matches("NN.*"))
                        {
                            nouns.add(data[1]);
                            nounCount++;
                        } // end if

                        if(data[2].matches("VB.*"))
                        {
                            verbs.add(data[1]);
                            verbCount++;
                        } // end if
                    } // end loop

                    buff.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                } // end try/catch

                // if there are nouns and verbs, output the them and their non-unique counts to a file
                if(!nouns.isEmpty() && !verbs.isEmpty())
                {
                    PrintWriter writer;
                    try {
                        writer = new PrintWriter("data/token_compact/" + num + "_token.txt");

                        writer.println(nounCount);
                        for (String x : nouns)
                            writer.println(x);

                        writer.println();
                        writer.println(verbCount);
                        for (String x : verbs)
                            writer.println(x);

                        writer.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } // end try/catch
                } // end if
            } // end if
        } // end loop
    } // end main
}

