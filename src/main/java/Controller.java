import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Controller {
    public static void main(String[] args) throws Exception {

        final int MAX_CRAWL_DEPTH = 0;
        final int NUMBER_OF_CRAWELRS = 1;
        final String CRAWL_STORAGE = "/home/elavelina/IdeaProjects/testCrawl/src/main/resources/result";

        /*
         * Instantiate crawl config
         */
        CrawlConfig config = new CrawlConfig();
        config.setCrawlStorageFolder(CRAWL_STORAGE);
        config.setMaxDepthOfCrawling(MAX_CRAWL_DEPTH);

        /*
         * Instantiate controller for this crawl.
         */
        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);


        /*
         * Add seed URLs
         */
        controller.addSeed("https://spbu.ru");

        /*
         * Start the crawl.
         */
        controller.start(MyCrawler.class, NUMBER_OF_CRAWELRS);

        System.out.println(MyCrawler.counter);

        try {
            FileOutputStream outputStream = new FileOutputStream("/home/elavelina/IdeaProjects/testCrawl/src/main/resources/output0.txt", true);
            DataOutputStream dataOutStream = new DataOutputStream(new BufferedOutputStream(outputStream));
            dataOutStream.writeUTF("Total link amount: " + MyCrawler.counter);
            dataOutStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}