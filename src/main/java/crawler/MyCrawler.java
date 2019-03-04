package crawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class MyCrawler extends WebCrawler {

    public static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/test";
    public static final String DB_USERNAME = "postgres";
    public static final String DB_PASSWORD = "password";

    public static Connection connection = connect();
    public static int counter = 0;


    public static String currentTable = "gazprom_football_com";
    public static String currentSeed = "www.gazprom-football.com";
    public static String currentDomain = "www.gazprom-football.com";

    private static Pattern FILE_ENDING_EXCLUSION_PATTERN = Pattern.compile(".*(\\.(" +
            "css|js" +
            "|bmp|gif|jpe?g|JPE?G|png|tiff?|ico|nef|raw" +
            "|mid|mp2|mp3|mp4|wav|wma|flv|mpe?g" +
            "|avi|mov|mpeg|ram|m4v|wmv|rm|smil" +
            "|pdf|doc|docx|pub|xls|xlsx|vsd|ppt|pptx" +
            "|swf" +
            "|zip|rar|gz|bz2|7z|bin" +
            "|xml|txt|java|c|cpp|exe" +
            "))$");

    private static boolean isLinkExternal(String link) {
        String href = link.toLowerCase();
        return !FILE_ENDING_EXCLUSION_PATTERN.matcher(href).matches() &&
                //!href.contains("gazprom.ru") &&
                !href.contains(currentDomain) &&
                !href.contains("fonts.google") &&
                !href.contains("googletagmanager");
    }


    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String href = url.getURL().toLowerCase();
        //return !FILE_ENDING_EXCLUSION_PATTERN.matcher(href).matches() && href.contains("www.gazprom.ru");
        return !FILE_ENDING_EXCLUSION_PATTERN.matcher(href).matches() && href.contains(currentSeed);
    }

    /**
     * This function is called when a page is fetched and ready
     * to be processed by the program.
     */
    @Override
    public void visit(Page page) {

        String url = page.getWebURL().getURL();

        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            //String text = htmlParseData.getText(); //extract text from page
            //String html = htmlParseData.getHtml(); //extract html from page
            Set<WebURL> links = htmlParseData.getOutgoingUrls();

            for (WebURL l : links) {

                String link = l.toString().split("://")[1];
                link = link.split("/")[0];

                boolean isStartedWithWWW = true;
                String linkWithWWWPrefix = link;

                if (!link.startsWith("www.")) {
                    isStartedWithWWW = false;
                    linkWithWWWPrefix = "www." + link;
                }

                String seed = url.split("://")[1].split("/")[0];

                if (isLinkExternal(link.toString())) {
//                    try {
//                        FileOutputStream outputStream = new FileOutputStream(Controller.CRAWL_STORAGE_FILE, true);
//                        DataOutputStream dataOutStream = new DataOutputStream(new BufferedOutputStream(outputStream));
//                        String toWrite = link.toString() + " " + "Current Depth: " + page.getWebURL().getDepth() +  "\n";
//                        dataOutStream.write(toWrite.getBytes());

                        counter++;
                        try {
                            try {
                                linkWithWWWPrefix = new java.net.URI(linkWithWWWPrefix).getPath();
                                seed = new java.net.URI(seed).getPath();
                            } catch (URISyntaxException e) {
                                e.printStackTrace();
                            }



                            if (isLinkInDB(seed, linkWithWWWPrefix)) {
                                if (isStartedWithWWW)
                                    updateLinkWithWWWPrefixToDb(seed, linkWithWWWPrefix);
                                else
                                    updateLinkToDb(seed, linkWithWWWPrefix);
                            }
                            else {
                                if (isStartedWithWWW)
                                    loadLinkToDb(counter, seed, linkWithWWWPrefix, 1, 0);
                                else
                                    loadLinkToDb(counter, seed, linkWithWWWPrefix, 0, 1);
                            }

                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

//                        dataOutStream.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                }

            }
        }
    }

    /**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     */
    public static Connection connect() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(DB_URL, DB_USERNAME, "password");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void updateLinkToDb(String seed, String path) throws SQLException {
        String SQL1 = "UPDATE " + currentTable  +
                      " SET page_amount = " +
                        "(SELECT page_amount FROM " + currentTable +
                            " WHERE link_path = '" + path + "' AND seed = '" + seed + "') + 1 " +
                      "WHERE link_path = '" + path + "'AND seed = '" + seed + "'";
        PreparedStatement pstmt1 = connection.prepareStatement(SQL1,
                Statement.RETURN_GENERATED_KEYS);
        pstmt1.executeUpdate();
    }

    public static void updateLinkWithWWWPrefixToDb(String seed, String path) throws SQLException {
        String SQL1 = "UPDATE " + currentTable +
                " SET page_amount_www = " +
                "(SELECT page_amount_www FROM " + currentTable +
                " WHERE link_path = '" + path + "' AND seed = '" + seed + "') + 1 " +
                "WHERE link_path = '" + path + "'AND seed = '" + seed + "'";
        PreparedStatement pstmt1 = connection.prepareStatement(SQL1,
                Statement.RETURN_GENERATED_KEYS);
        pstmt1.executeUpdate();
    }

    public void loadLinkToDb(int linkId, String seed, String path, int pageAmountWWW, int pageAmount) throws SQLException {

        String SQL = "INSERT INTO " + currentTable + "(link_id, seed, link_path, page_amount_www, page_amount) "
                + "VALUES(?,?,?,?,?)";



            PreparedStatement pstmt = connection.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setInt(1, linkId);
            pstmt.setString(2, seed);
            pstmt.setString(3, path);
            pstmt.setInt(4, pageAmountWWW);
            pstmt.setInt(5, pageAmount);

            pstmt.executeUpdate();
        }

    public boolean isLinkInDB(String seed, String link) throws SQLException {
        String SQL = "SELECT link_path FROM " + currentTable + " WHERE link_path = '" + link + "' AND " +
                "seed = '" + seed + "'";
        PreparedStatement pstmt = connection.prepareStatement(SQL);
        ResultSet resultSet = pstmt.executeQuery();

        return resultSet.next();
    }

//    public Map<String, String> getLinkList() throws SQLException {
//        Map<String, String> linkList = new HashMap<>();
//
//        String SQL_getLinks = "SELECT seed_url, tables_url FROM link_list";
//        PreparedStatement pstmt = connection.prepareStatement(SQL_getLinks);
//        ResultSet resultSet = pstmt.executeQuery();
//
//        while (resultSet.next()) {
//            linkList.put(resultSet.getString("seed_url"), resultSet.getString("tables_url"));
//        }
//
//        return linkList;
//
//    }

//    public static void main(String[] args) {
//        try {
//            updateLinkToDb( "www.gazprom-agnks.ru");
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
}
