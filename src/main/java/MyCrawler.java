import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Set;
import java.util.regex.Pattern;

public class MyCrawler extends WebCrawler {

    public static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/test";
    public static final String DB_USERNAME = "postgres";
    public static final String DB_PASSWORD = "password";

    public static int counter = 0;

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
                !href.contains("gazprom") &&
                !href.contains("fonts.google") &&
                !href.contains("googletagmanager");
    }

    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String href = url.getURL().toLowerCase();
        return !FILE_ENDING_EXCLUSION_PATTERN.matcher(href).matches();
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
            String text = htmlParseData.getText(); //extract text from page
            String html = htmlParseData.getHtml(); //extract html from page
            Set<WebURL> links = htmlParseData.getOutgoingUrls();

            for (WebURL link : links) {

                if (isLinkExternal(link.toString())) {
//                    try {
//                        FileOutputStream outputStream = new FileOutputStream(Controller.CRAWL_STORAGE_FILE, true);
//                        DataOutputStream dataOutStream = new DataOutputStream(new BufferedOutputStream(outputStream));
//                        String toWrite = link.toString() + " " + "Current Depth: " + page.getWebURL().getDepth() +  "\n";
//                        dataOutStream.write(toWrite.getBytes());
                        counter++;
                        try {
                            loadLinkToDb(counter, url, link.toString(), page.getWebURL().getDepth());
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
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USERNAME, "password");
    }

    public void loadLinkToDb(int linkId, String seed, String path, int pageLevel) throws SQLException {

        String SQL = "INSERT INTO table_gazprom1(link_id, seed, link_path, page_level) "
                + "VALUES(?,?,?,?)";

        Connection conn = connect();
        PreparedStatement pstmt = conn.prepareStatement(SQL,
                Statement.RETURN_GENERATED_KEYS);

        pstmt.setInt(1, linkId);
        pstmt.setString(2, seed);
        pstmt.setString(3, path);
        pstmt.setInt(4, pageLevel);

        pstmt.executeUpdate();

        conn.close();
    }
}
