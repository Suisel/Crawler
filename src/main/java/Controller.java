import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class Controller {

    final static int MAX_CRAWL_DEPTH = 2;
    final static int NUMBER_OF_CRAWELRS = 10;
    final static String CRAWL_STORAGE = "C:\\Users\\Viktor\\Desktop\\crawlResults";
    final static String CRAWL_STORAGE_FILE = "C:\\Users\\Viktor\\Desktop\\crawlResults\\output0.txt";
    final static String SEED_URL = "https://spbu.ru";

    public static void main(String[] args) throws Exception {

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
        controller.addSeed(SEED_URL);

        /*
         * Start the crawl.
         */
        controller.start(MyCrawler.class, NUMBER_OF_CRAWELRS);
    }
}