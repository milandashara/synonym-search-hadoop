package searchSynonym;

import hive.client.HiveJdbcClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import synonymSearchEngine.mapReduce.SynonymMapReduce;

/**
 * Servlet implementation class SearchSynonym
 */
@WebServlet(name = "searchSynonym", urlPatterns = { "/searchSynonym" })
public class SearchSynonym extends HttpServlet {
	private static final long serialVersionUID = 1L;

    /**
     * Default constructor. 
     */
    public SearchSynonym() {
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String searchKeyword=request.getParameter("searchTextBox").toString();
		HiveJdbcClient hjc=new HiveJdbcClient();
		String s[]=hjc.searchString(searchKeyword);
		List<String> synonyms=new ArrayList<String>();
		if(s==null)
		{
			SynonymMapReduce mapReduce=new SynonymMapReduce();
			mapReduce.startSynonymSearchMapReduce(searchKeyword);
			try {
				Thread.sleep(3000l);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			s=hjc.searchString(searchKeyword);
			if(s==null)
			{
				synonyms.add("No Synonyms Found");
			}
			else
			{
				synonyms=Arrays.asList(s);
				
			}
		}
		else
		{
			synonyms=Arrays.asList(s);
			
		}
		
		for(String synonym:synonyms)
		{
			response.getOutputStream().println(synonym);
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
