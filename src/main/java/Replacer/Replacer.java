package Replacer;

import crawler.MyCrawler;

import java.sql.*;
import java.util.*;

import static crawler.MyCrawler.connect;
import static crawler.MyCrawler.currentTable;

public class Replacer {

    private static Connection connection = connect();
    private static String SQL_GROUP_BY = "select link_path, sum(page_amount) as sum_amount " +
                            "from final_table group by link_path";

    public static void main(String[] args) throws SQLException {

        updateTable(calculateUniqueLinks(getGroupedLinks()));

    }

    private static Map<String, Integer> getGroupedLinks() throws SQLException {
        try (Connection connection = MyCrawler.connect()) {
            Map<String, Integer> groupedLinks;
            try (PreparedStatement pstmt1 = connection.prepareStatement(SQL_GROUP_BY,
                    Statement.RETURN_GENERATED_KEYS)) {
                try (ResultSet resultSet = pstmt1.executeQuery()) {
                    groupedLinks = new HashMap<>();
                    while (resultSet.next()) {
                        groupedLinks.put(resultSet.getString("link_path"), resultSet.getInt("sum_amount"));
                    }
                }
            }
            return groupedLinks;
        }
    }

    private static Set<String> calculateUniqueLinks(Map<String, Integer> groupedLinks) {
        Set<String> uniqueLinks = new HashSet<>();
        Set<String> linksSet = groupedLinks.keySet();

        for (String link : linksSet) {

            String pairLink = getPairLink(link);

            if (!groupedLinks.containsKey(pairLink)) {
                uniqueLinks.add(link);
                continue;
            }

            if (groupedLinks.get(link) > groupedLinks.get(pairLink)) {
                uniqueLinks.add(link);
            } else {
                uniqueLinks.add(pairLink);
            }
        }

//
//        linksSet.removeAll(uniqueLinks);
//        List<String> test = new ArrayList<>(linksSet);
//        java.util.Collections.sort(test);

        return uniqueLinks;
    }

    private static void updateTable(Set<String> uniqueLinks) throws SQLException {
        for (String link : uniqueLinks) {

            String pairLink = getPairLink(link);

            try (Connection connection = MyCrawler.connect()) {
                String SQL1 = "UPDATE final_table" +
                        " SET link_path = '" + link + "'" +
                        " WHERE link_path = '" + pairLink + "'";
                try (PreparedStatement pstmt1 = connection.prepareStatement(SQL1)) {
                    pstmt1.executeUpdate();
                }
            }
        }
    }

    private static String getPairLink(String link) {
        String pairLink;

        if (link.startsWith("www.")) {
            pairLink = link.split("www.")[1];
        } else {
            pairLink = "www." + link;
        }

        return pairLink;
    }
}
