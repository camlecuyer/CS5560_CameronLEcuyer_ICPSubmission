import java.io.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class RESTClientGet {
	// this code retrieves the words that are medical in nature and saves them to a file, it also outputs the total
	// medical words per abstract
    public static void main(String[] args)
	{
		if(args.length<2)
		{
			System.out.println("\n$ java RESTClientGet [Bioconcept] [Inputfile] [Format]");
			System.out.println("\nBioconcept: We support five kinds of bioconcepts, i.e., Gene, Disease, Chemical, Species, Mutation. When 'BioConcept' is used, all five are included.\n\tInputfile: a file with a pmid list\n\tFormat: PubTator (tab-delimited text file), BioC (xml), and JSON\n\n");
		}
		else
		{
			String Bioconcept=args[0];
			String Inputfile=args[1];
			String Format="PubTator";

			if(args.length > 2)
			{
				Format=args[2];
			}
			
			try {
				//pmids
				BufferedReader fr = new BufferedReader(new FileReader(Inputfile));
				String pmid = "";
                PrintWriter writer = new PrintWriter("data/med.txt");

				while((pmid = fr.readLine()) != null)
				{
                    int count = 0;
					URL url_Submit;
					url_Submit = new URL("https://www.ncbi.nlm.nih.gov/CBBresearch/Lu/Demo/RESTful/tmTool.cgi/" + Bioconcept + "/" + pmid + "/"+Format+"/");
					HttpURLConnection conn_Submit = (HttpURLConnection) url_Submit.openConnection();
					conn_Submit.setDoOutput(true);
					BufferedReader br_Submit = new BufferedReader(new InputStreamReader(conn_Submit.getInputStream()));
					String line="";

					int pass = 0;

					while((line = br_Submit.readLine()) != null)
					{
						// ignores the title and abstract returned by the REST call and parses the tab separated data
					    if(pass > 1 && line.compareTo("") != 0)
                        {
                        	// saves the word and type to a file
                            String[] medData = line.split("\t", -1);
                            writer.println(medData[3] + " " + medData[4]);
                            count++;
                        } // end if
                        pass++;
					}

					writer.println();
					conn_Submit.disconnect();
					System.out.println(pmid + " "+ count);
				}
				fr.close();
				writer.close();
			}
			catch (MalformedURLException e) 
			{
				e.printStackTrace();
			}
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
    }
}