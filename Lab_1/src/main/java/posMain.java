import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

public class posMain {
    static final int FILE_ID = 11;

    // This code retrieves the abstracts from a file, and breaks them up into their tokens and lemmatizes them, it
    // also calculate the part of speech of each word, and saves it to a file
    public static void main(String args[]) {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        String text = null;

        // reads abstract all at once
        try
        {
            text = new String(Files.readAllBytes(Paths.get("data/abstract_text/" + FILE_ID + ".txt")));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        } // end try/catch

        // if there is a text present, it tries to do NLP
        if(text != null) {
            // create an empty Annotation just with the given text
            Annotation document = new Annotation(text);

            // run all Annotators on this text
            pipeline.annotate(document);

            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

            int count = 0;
            PrintWriter writer;

            // counts the words in the abstract, and outputs the token, lemma, and PoS to a file
            try {
                writer = new PrintWriter("data/token_results/" + FILE_ID + "_token.txt");

                for (CoreMap sentence : sentences)
                {
                    // traversing the words in the current sentence
                    // a CoreLabel is a CoreMap with additional token-specific methods
                    for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class))
                    {
                        String word = token.get(CoreAnnotations.TextAnnotation.class);
                        if(word.compareTo(",") != 0 && word.compareTo(".") != 0 && word.compareTo(":") != 0
                                && word.compareTo(";") != 0 && word.compareTo("'") != 0 && word.compareTo("%") != 0
                                && word.compareTo("=") != 0 && word.compareTo("''") != 0 && word.compareTo("``") != 0)
                        {
                            writer.print(word + " ");

                            String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
                            writer.print(lemma + " ");

                            String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                            writer.println(pos);

                            count++;
                        } // end if
                    } // end loop
                } // end loop
                writer.close();
                System.out.println("Total words: " + count);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } // end try/catch
        } // end if
    } // end main
}
