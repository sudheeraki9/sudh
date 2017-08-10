package com.servlet;

import java.util.logging.Logger;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.process.MailUser;
import com.utils.BigQueryManager;
import com.utils.Constants;

/**
 * Servlet for generating the final tables using the provided views.
 * @author PXB8449
 *
 */
@WebServlet(
	    name = "JDA_VIEW_SERVLET",
	    urlPatterns = {
	      "/run_main_jda_views"})
public class JDA_VIEW_SERVLET extends HttpServlet implements Constants {

  /**
   * The Serial Version ID.
   */
  private static final long serialVersionUID = -1702616660888933160L;

  /**
   * Logging tool.
   */
  static Logger log = Logger.getLogger(IX_SPC_FIXTURServlet.class.getName());
  
  /**
   * Process the get request.
   */
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    String[] tableList = new String[] {
      "IX_SPC_FIXTURE_VIEW",
      "IX_SPC_PERFORMANCE_VIEW",
      "IX_SPC_PLANOGRAM_VIEW",
      "IX_SPC_POSITION_VIEW",
      "IX_SPC_PRODUCT_VIEW",
      "IX_SPC_SEGMENT_VIEW"};

    // Generate the secondary tables.
    log.info("Receiving request for creating JDA tables from views.");
    for(int i = 0; i < tableList.length; i++) {

      String tableName = tableList[i];
      
      log.info(String.format(
        "Running table query for \"%s\"",
        tableName));
      try {
        String query = createViewQuery(tableName);
        log.info(String.format(
          "\nCreated the table query \"%s\" for \"%s\"",
          query,
          tableName));

        BigQueryManager.deleteAndCreateTable("JDA", tableName, query);
      } catch (Exception e) {
        String errMsg = String.format(
          "An exception occured when creating the main table \"%s\"\nError: \"%s\"",
          tableName,
          e.getMessage());
        log.severe(errMsg);
        MailUser.sendMail(
          PROJECT_NAME + " : IX_SPC_PRODUCT Notification",
          errMsg);
        e.printStackTrace();
      } 
    }
  }
  
  /**
   * Create a query for Atom View.
   *
   * @param tableName - The name of the table to create the query for.
   * @return - The query for Atom View.
   */
  protected static String createViewQuery(String tableName) {
    return
      "SELECT * FROM ["+
        Constants.PROJECT_NAME + ":" + "JDA_Views"  + "." + tableName +
        "_V]";
  }
}
