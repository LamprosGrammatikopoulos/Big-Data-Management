/*

Creators:
Lampros Grammatikopoulos, AM:2022201800038
Kwnstantinos Kolotouros, AM:2022201800090

*/

package jsonalternation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

public class JsonAlternation
{
    public static void main(String[] args) throws FileNotFoundException
    {
        try
        {
            File inpf = new File("json.json");
            Scanner s = new Scanner(inpf);
            
            File outf = new File("alteredjson.json");
            PrintWriter pw = new PrintWriter(outf);
            
            while(s.hasNextLine())
            {
                String line = s.nextLine();
                if( !(line.contains("[") || line.contains("{") || line.contains("}") || line.contains("]")) )
                {
                    String category[] = line.split(":");
                    if(category[1].equals(" \"\",") == false)           //if field isn't empty
                    {
                        if(category[0].contains("director") || category[0].contains("cast") || category[0].contains("country") || category[0].contains("listed_in") || category[0].contains("date_added"))
                        {
                            String fields[] = category[1].split(", ");
                            for(int i=0;i<fields.length;i++)
                            {
                                System.out.println("11111"+fields[i]);
                            }
                            if(!category[0].contains("date_added"))
                            {
                                fields[0] = RemoveLastChar(fields[0]);
                            }
                            fields[fields.length-1] = RemoveLastChar(fields[fields.length-1]);
                            for(int i=0;i<fields.length;i++)
                            {
                                System.out.println("22222"+fields[i]);
                            }
                            line = category[0] + ": [" + fields[0] + "\"";
                            for(int i=1;i<fields.length;i++)
                            {
                                if(i == fields.length-1)
                                {
                                    line = line + ",\"" + fields[i];
                                }
                                else
                                {
                                    line = line + ",\"" + fields[i] + "\"";
                                }
                            }
                            line = line + " ],";
                            if(fields.length == 1)
                            {
                                line = line.replace(" ]","");         //One field
                                line = line.replace(" [","");
                            }
                        }
                        if(category[0].contains("release_year"))      //integer to string
                        {
                            String fields[] = category[1].split(", ");
                            fields[0] = RemoveLastChar(fields[0]);
                            fields[0] = RemoveFirstChar(fields[0]);
                            line = category[0] + ": \"" + fields[0] + "\",";
                        }
                    }
                }
                pw.println(line);
            }
            s.close();
            pw.close();
        }
        catch (FileNotFoundException e)
        {
            System.out.println(e);
        }
        catch (IOException e)
        {
            System.out.println(e);
        }
    }   
    
    public static String RemoveLastChar(String str)
    {
        StringBuilder sb=new StringBuilder(str);
        
        sb.deleteCharAt(str.length()-1);
        return sb.toString();
    }
    
    public static String RemoveFirstChar(String str)
    {
        StringBuilder sb=new StringBuilder(str);
        
        sb.deleteCharAt(0);
        return sb.toString();
    } 
}