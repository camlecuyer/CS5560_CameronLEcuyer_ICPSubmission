package pubmedabstract;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class XMLReaderForAbstracts4 {

	public static void main(String[] args)
	{
		try {
			for(int i=1;i<100;i++)
			{
			 File file = new File("new_data_allergy/abstracts/" + i +".xml");

			 if(file.exists())
			  {
				  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				  DocumentBuilder db = dbf.newDocumentBuilder();
				  Document doc = db.parse(file);
				  doc.getDocumentElement().normalize();
				  System.out.println("Root element " + doc.getDocumentElement().getNodeName());
				  NodeList abstractBlock = doc.getElementsByTagName("AbstractText");

				  File f = new File("new_data_allergy/abstract_text/"+i+".txt");
				  FileWriter fw= new FileWriter(f.getAbsoluteFile());

				  for(int temp = 0; temp < abstractBlock.getLength(); temp++)
				  {
				  	Node abst = abstractBlock.item(temp);
				  	if(abst.getNodeType() == Node.ELEMENT_NODE)
					{
						Element e = (Element) abst;

						fw.append(e.getTextContent());
					}
				  }

				  fw.close();
				}
			}
		} catch (DOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}