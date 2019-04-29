package Processor;

import crawler.MyCrawler;

import java.sql.*;
import java.util.*;

import static crawler.MyCrawler.connect;

public class Processor {

    private static Connection connection = connect();
    private static String SQL_ALL_OUTGOING_LINKS = "SELECT seed, sum(page_amount) as amount " +
                                                   "FROM final_table " +
                                                   "group by seed";
    private static String SQL_GAZPROM_OUTGOING_LINKS = "SELECT seed, link_path, sum(page_amount) as amount " +
                                                       "FROM final_table " +
                                                       "where link_path in " +
                                                           "(select split_part(regexp_replace(rtrim(trim(seed_url), '/'), '^https?://?' , ''), '/', 1) " +
                                                             "from link_list) group by seed, link_path";

    private static String SQL_OUTGOING_LINKS = "SELECT seed, link_path, page_amount as amount "
            + "FROM final_table";

    private static String SQL_LINK_LIST = "SELECT seed_url FROM link_list";

    public static void main(String[] args) throws SQLException {
        Map<String, Integer> allOutgoingLinks = getAllOutgoingLinks();
        List<Linker> gazpromOutgoingLinks = getGazpromOutgoingLinks();
        List<Linker> linkers = testIfDuplicate(gazpromOutgoingLinks);

        getCorrectOutgoingLinks(getAllLinksFromDb());

        int[][] linkMatrix = createLinkMatrix(getCorrectOutgoingLinks(getAllLinksFromDb()));
    }

    private static List<Linker> getAllLinksFromDb() throws SQLException {
        List<Linker> outgoingLinks;
        try (Connection connection = MyCrawler.connect()) {
            try (PreparedStatement pstmt1 = connection.prepareStatement(SQL_OUTGOING_LINKS,
                    Statement.RETURN_GENERATED_KEYS)) {
                try (ResultSet resultSet = pstmt1.executeQuery()) {
                    outgoingLinks = new ArrayList<>();
                    while (resultSet.next()) {
                        Linker linker = new Linker();
                        linker.setSeed(resultSet.getString("seed"));
                        linker.setLinkPath(resultSet.getString("link_path"));
                        linker.setAmount(resultSet.getInt("amount"));
                        outgoingLinks.add(linker);
                    }
                }
            }
        }
        return outgoingLinks;
    }

    private static List<Linker> getCorrectOutgoingLinks(List<Linker> outgoingLinks) {
        List<Linker> correctOutgoingLinks = new ArrayList<>();

        final Set<String> communitySet = new HashSet<>();

        outgoingLinks.forEach(linker -> communitySet.add(linker.getSeed()));

        outgoingLinks.forEach(linker -> {
            if (communitySet.contains(linker.getLinkPath())) {
                correctOutgoingLinks.add(linker);
            }
        });

        return correctOutgoingLinks;
    }

    private static List<Linker> testIfDuplicate(List<Linker> gazpromOutgoingLinks) {

        List<Linker> falseLinkers = new ArrayList<>();

        for (Linker linker : gazpromOutgoingLinks) {
                int counter = -1;
            for (Linker linkerClone : gazpromOutgoingLinks) {
                if (linker.getSeed().equals(linkerClone.getSeed()) && linker.getLinkPath().equals(linkerClone.getLinkPath())) {
                    counter++;
                }
            }

            if (counter >= 1) {
                falseLinkers.add(new Linker(linker.getSeed(), linker.getLinkPath(), counter));
            }
        }

        return falseLinkers;
    }

    private static int[][] createLinkMatrix(List<Linker> linkerList) {

        final Set<String> linkSet = new HashSet<>();

        linkerList.forEach(linker -> { linkSet.add(linker.getSeed());
            linkSet.add(linker.getLinkPath());});

        String[] linkVector = linkSet.toArray(new String[linkSet.size()]);

        int[][] linkMatrix = new int[linkVector.length][linkVector.length];

        for (Linker linker : linkerList) {

            int seedIndex = -1;
            int linkPathIndex = -1;

            for (int i = 0; i < linkVector.length; i++) {

                if (linker.getSeed().equals(linkVector[i])) {
                    seedIndex = i;
                }
                if (linker.getLinkPath().equals(linkVector[i])) {
                    linkPathIndex = i;
                }
            }

            linkMatrix[seedIndex][linkPathIndex] = 1;
        }

        return linkMatrix;
    }

    private static Map<String, Integer> getAllOutgoingLinks() throws SQLException {
        try (Connection connection = MyCrawler.connect()) {
            Map<String, Integer> allOutgoingLinks;
            try (PreparedStatement pstmt1 = connection.prepareStatement(SQL_ALL_OUTGOING_LINKS,
                    Statement.RETURN_GENERATED_KEYS)) {
                try (ResultSet resultSet = pstmt1.executeQuery()) {
                    allOutgoingLinks = new HashMap<>();
                    while (resultSet.next()) {
                        allOutgoingLinks.put(resultSet.getString("seed"), resultSet.getInt("amount"));
                    }
                }
            }
            return allOutgoingLinks;
        }
    }

    private static List<Linker> getGazpromOutgoingLinks() throws SQLException {
        try (Connection connection = MyCrawler.connect()) {
            List<Linker> gazpromOutgoingLinks;
            try (PreparedStatement pstmt1 = connection.prepareStatement(SQL_GAZPROM_OUTGOING_LINKS,
                    Statement.RETURN_GENERATED_KEYS)) {
                try (ResultSet resultSet = pstmt1.executeQuery()) {
                    gazpromOutgoingLinks = new ArrayList<>();
                    while (resultSet.next()) {
                        Linker linker = new Linker();
                        linker.setSeed(resultSet.getString("seed"));
                        linker.setLinkPath(resultSet.getString("link_path"));
                        linker.setAmount(resultSet.getInt("amount"));
                        gazpromOutgoingLinks.add(linker);
                    }
                }
            }
            return gazpromOutgoingLinks;
        }
    }



}
