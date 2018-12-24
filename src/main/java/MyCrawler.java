import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static java.sql.DriverManager.getConnection;

public class MyCrawler extends WebCrawler {
//    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MyCrawler.class);

    public static int counter = 0;

    private static Connection conn = connect();

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

    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String href = url.getURL().toLowerCase();
        return !FILE_ENDING_EXCLUSION_PATTERN.matcher(href).matches() && !href.startsWith("https://spbu.ru/");
    }

    /**
     * This function is called when a page is fetched and ready
     * to be processed by the program.
     */
    @Override
    public void visit(Page page) {
        String url = page.getWebURL().getURL();
        //logger.info("URL: " + url);
        //System.out.println("URL: " + url);
        try {
            FileOutputStream outputStream = new FileOutputStream("/home/elavelina/IdeaProjects/testCrawl/src/main/resources/output0.txt", true);
            DataOutputStream dataOutStream = new DataOutputStream(new BufferedOutputStream(outputStream));
            dataOutStream.writeUTF("URL: " + url + "\n");
            dataOutStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            String text = htmlParseData.getText(); //extract text from page
            String html = htmlParseData.getHtml(); //extract html from page
            Set<WebURL> links = htmlParseData.getOutgoingUrls();

            //System.out.println("---------------------------------------------------------");
//            System.out.println("Page URL: " + url);
//            System.out.println("Text length: " + text.length());
//            System.out.println("Html length: " + html.length());
            //logger.info("Number of outgoing links: " + links.size());
            try {
                FileOutputStream outputStream = new FileOutputStream("/home/elavelina/IdeaProjects/testCrawl/src/main/resources/output0.txt", true);
                DataOutputStream dataOutStream = new DataOutputStream(new BufferedOutputStream(outputStream));
                dataOutStream.writeUTF("Number of outgoing links: " + links.size());
                dataOutStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //System.out.println("Number of outgoing links: " + links.size());
            for (WebURL link : links) {
                //System.out.println(link);
                //logger.info(link.toString());
                if (isLinkExternal(link.toString())) {
                    try {
                        FileOutputStream outputStream = new FileOutputStream("/home/elavelina/IdeaProjects/testCrawl/src/main/resources/output0.txt", true);
                        DataOutputStream dataOutStream = new DataOutputStream(new BufferedOutputStream(outputStream));
                        dataOutStream.writeUTF("\n" + link.toString());
                        counter++;
                        try {
                            loadLinkToDb(counter, url, link.toString(), 0);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        dataOutStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                FileOutputStream outputStream = new FileOutputStream("/home/elavelina/IdeaProjects/testCrawl/src/main/resources/output0.txt", true);
                DataOutputStream dataOutStream = new DataOutputStream(new BufferedOutputStream(outputStream));
                dataOutStream.writeUTF("\n---------------------------------------------------------\n");
                dataOutStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //System.out.println("---------------------------------------------------------");

            //if required write content to file
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
             connection = getConnection("jdbc:postgresql://127.0.0.1:5432/test", "postgres", "password");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public void loadLinkToDb(int linkId, String seed, String path, int pageLevel) throws SQLException {

        String SQL = "INSERT INTO testtable(link_id, seed, link_path, page_level) "
                + "VALUES(?,?,?,?)";

        PreparedStatement pstmt = conn.prepareStatement(SQL,
                Statement.RETURN_GENERATED_KEYS);

        pstmt.setInt(1, linkId);
        pstmt.setString(2, seed);
        pstmt.setString(3, path);
        pstmt.setInt(4, pageLevel);

        pstmt.executeUpdate();
            // check the affected rows
    }

    private static boolean isLinkExternal(String link) {
        String href = link.toLowerCase();
        return !FILE_ENDING_EXCLUSION_PATTERN.matcher(href).matches() &&
                !href.contains("spbu.ru/") &&
                !href.contains("fonts.google") &&
                !href.contains("googletagmanager") ;
    }
}
