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

    private static String SQL_TIC = "select seed, tic from link_list_final";

    private static String[] linkVector;
    private static double[] ticVector;
    private static int[] lVector;
    private static int[][] linkMatrix;
    private static int permutationCounter = 0;

    private static int[][] real = {{0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0},
            {1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1},
            {0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0}};

    private static int[][] opt = {{0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0},
            {1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1},
            {0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0}};

    private static int[][] opt2 = {{0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0},
            {1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,0,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1},
            {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0},
            {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0}};

    private static double[] newTicVector = {
        0.168989570436847,
        0.0849108975004589,
        0.0127771067747031,
        -1.16375148087013e-06,
        0.0758999809669787,
        0.0422930312173663,
        0.0893454991601038,
        1.11054974160474,
        0.00554200842385281,
        0.00131446173181065,
        0.816281547673879,
        0.0701674127814248,
        157.142778426148,
        0.308886659202174,
        2.49972449151898,
        0.00422151821846721,
        0.0316829243033464,
        0.248981315642072,
        0.0403977138159727,
        2.61904723505617,
        0.153149940872549,
        0.373926899610373,
        0.0304131784136663,
        0.00522623937906526,
        0.0525868914671976,
        0.666519728810122,
        0.0878558723232282,
        0.0730168923658182,
        0.0401085985552936,
        0.00324862203725338,
        4.70911092409853,
        9.99889796607593,
        0.0926100860408518,
        0.535735767827577,
        0.00157166465271444,
        0.0103462170339914,
        0.0202060921187760,
        0.00102584188978728,
        0.0397923363204510,
        9.99889796883763,
        0.0417922988577553,
        8.47666521626843e-06,
        0.0159162556243081,
        0.0256305975029944,
        0.0456621004566210,
        0.0506976436634544,
        0.147042617148175,
        0.000928321963183132,
        0.00847874373032072,
        0.100884986418043,
        0.0129942392671969,
        0.00492894884066897,
        0.0415279776806258,
        0.0230699743894023,
        0.000234212851880958,
        0.0245972081900968,
        0.102902261459107,
        0.0389514770201062,
        0.00339984126014806,
        0.209344267099529,
        0.0278791425607008,
        0.122117004782740,
        0.0484634628908161,
        0.0282598221950133,
        0.0284868887922391,
        1.88886439924613,
        5.23799028248342,
        0.0706747480942371,
        0.0619912559085625,
        0.171033110572414,
        0.00138650823119435,
        0.00124457281134966,
        0.00485910718757585,
        0.0364060417200118,
        0.0257045251406363,
        0.00597553265536107,
        0.170107392040291,
        0.249972449151898,
        0.420489089843466,
        0.0887830221980843,
        0.0666605732349724,
        0.775876905793071,
        0.0999113921343894,
        0.0874333876018417,
        0.0666111240077483,
        0.105046979863990,
        0.0502242719862652,
        0.0133040215388802,
        0.315777873327115,
        0.0408118284329629,
        0.229707347528091,
        6,
        0.0598396927965930,
        0.0611076856309659,
        0.147162038872383,
        0.137229760147316,
        4.99222756981749,
        0.0333522661533960,
        0.0691975874724928,
        0.168418727298101,
        9.99779593215184
    };


    public static void main(String[] args) throws SQLException {
        List<Linker> gazpromOutgoingLinks = getGazpromOutgoingLinks();
        List<Linker> linkers = testIfDuplicate(gazpromOutgoingLinks);

        List<Linker> correctOutgoingLinks = getCorrectOutgoingLinks(getAllLinksFromDb());
        linkMatrix = createLinkMatrix(correctOutgoingLinks);

        createTICVector();
        createLVector();

        ticVector = newTicVector;

        for (int i = 0; i < linkMatrix.length; i++) {
            System.out.println();
            for (int j = 0; j < linkMatrix.length; j++) {
                System.out.print(linkMatrix[i][j] + " ");
            }
        }

        System.out.println();

        System.out.println("TICVECTOR:" + ticVector.length);
        for (int j = 0; j < ticVector.length; j++) {
            System.out.print(ticVector[j] + " ");
        }

        System.out.println();

        System.out.println("LVECTOR:" + lVector.length);
        for (int j = 0; j < lVector.length; j++) {
            System.out.print(lVector[j] + " ");
        }

        cleanupTICZeroes();
        cleanupLinkMatrix();

        for (int i = 0; i < linkMatrix.length; i++) {
            System.out.println();
            for (int j = 0; j < linkMatrix.length; j++) {
                System.out.print(linkMatrix[i][j] + " ");
            }
        }

        System.out.println();

        System.out.println("TICVECTOR:" + ticVector.length);
        for (int j = 0; j < ticVector.length; j++) {
            System.out.print(ticVector[j] + " ");
        }

        System.out.println();

        System.out.println("LVECTOR:" + lVector.length);
        for (int j = 0; j < lVector.length; j++) {
            System.out.print(lVector[j] + " ");
        }
        System.out.println();
//        int[][] linkMatrix2 = new int[34][34];
//        for (int i = 0; i < 34; i++) {
//            for (int j = 0; j < 34; j++) {
//                linkMatrix2[i][j] = linkMatrix[i][j];
//            }
//        }
//        linkMatrix = linkMatrix2;
  //      int[][] rebuildMatrix = rebuildMatrix(linkMatrix);

      /*  for (int i = 0; i < linkVector.length; i++) {
            System.out.println(linkVector[i] + " " + lVector[i]);
        }*/

//        for (int i = 0; i < rebuildMatrix.length; i++) {
//            System.out.println();
//            for (int j = 0; j < rebuildMatrix.length; j++) {
//                System.out.print(rebuildMatrix[i][j] + " ");
//            }
//        }

        System.out.println("FINAL RESULTS:");

        System.out.println(getFuncValueMax(opt2));
        System.out.println(getFuncValueMax(real));
        System.out.println(getDeviation(real, opt2));

    }

    private static void cleanupTICZeroes() {
        List<Integer> indexes = new ArrayList<>();

        for (int i = 0; i < ticVector.length; i++) {
            if (ticVector[i] < 0.06) {
                indexes.add(i);
            }
        }
        deleteRows(indexes);
    }

    private static void cleanupLinkMatrix() {
        boolean isNeededToCleanUp = false;
        List<Integer> indexes = new ArrayList<>();

        for (int i = 0; i < linkMatrix.length; i++) {
            int sum = 0;
            for (int j = 0; j < linkMatrix.length; j++) {
                sum = sum + linkMatrix[i][j];
            }
            if (sum == 0) {
                isNeededToCleanUp = true;
                indexes.add(i);
            }
        }

        while (isNeededToCleanUp) {
            deleteRows(indexes);

            indexes.clear();
            isNeededToCleanUp = false;

            for (int i = 0; i < linkMatrix.length; i++) {
                int sum = 0;
                for (int j = 0; j < linkMatrix.length; j++) {
                    sum = sum + linkMatrix[i][j];
                }
                if (sum == 0) {
                    isNeededToCleanUp = true;
                    indexes.add(i);
                }
            }
        }
    }

    private static void deleteRows(List<Integer> indexes) {
        String[] newLinkVector = new String[linkVector.length - indexes.size()];
        double[] newTicVector = new double[ticVector.length - indexes.size()];
        int[] newLVector = new int[lVector.length - indexes.size()];
        int[][] newLinkMatrix = new int[linkMatrix.length - indexes.size()][linkMatrix.length - indexes.size()];

        int currentIndex = 0;

        for (int i = 0; i < linkVector.length; i++) {
            if (!indexes.contains(i)) {

                newLinkVector[currentIndex] = linkVector[i];
                newTicVector[currentIndex] = ticVector[i];
                newLVector[currentIndex] = lVector[i];

                int currentIndexj = 0;
                for (int j = 0; j < linkVector.length; j++) {
                    if (!indexes.contains(j)) {
                        newLinkMatrix[currentIndex][currentIndexj] = linkMatrix[i][j];
                        currentIndexj++;
                    }
                }

                currentIndex++;
            }
        }

        linkVector = newLinkVector;
        ticVector = newTicVector;
        lVector = newLVector;
        linkMatrix = newLinkMatrix;
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

        linkVector = linkSet.toArray(new String[linkSet.size()]);

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

        for (int i = 0; i < linkVector.length; i++) {
            linkMatrix[i][i] = 0;
        }

        return linkMatrix;
    }

    private static void createTICVector() throws SQLException {

        Map<String, Integer> tic = getTic();

        ticVector = new double[linkVector.length];

        for (String ticLink : tic.keySet()) {
            for (int i = 0; i < linkVector.length; i++) {
                if (linkVector[i].equals(ticLink)) {
                    ticVector[i] = tic.get(ticLink);
                }
            }
        }
        double[] ticVector2 = new double[linkVector.length];
        for (int i = 0; i < linkVector.length; i++) {
            ticVector2[i] = ticVector[i];
        }
        ticVector = ticVector2;
    }

    private static void createLVector() throws SQLException {
        Map<String, Integer> allOutgoingLinks = getAllOutgoingLinks();

        lVector = new int[linkVector.length];

        for (String link : allOutgoingLinks.keySet()) {
            for (int i = 0; i < linkVector.length; i++) {
                if (linkVector[i].equals(link)) {
                    lVector[i] = allOutgoingLinks.get(link);
                }
            }
        }
        int[] lVector2 = new int[linkVector.length];
        for (int i = 0; i < linkVector.length; i++) {
            lVector2[i] = lVector[i];
        }
        lVector = lVector2;
    }

    private static double getKCoefValue(int[][] linkMatrix) {

        double k = 0.0;

        for (double tic : ticVector) {
            k += tic;
        }


        for (int i = 0; i < ticVector.length; i++) {
            double addend = 0.0;

            for (int j = 0; j < ticVector.length; j++) {
                addend += linkMatrix[i][j];
            }

            addend = (addend * ticVector[i]) / lVector[i];

            k += addend;
        }

        k /= ticVector.length;

        return k;
    }

    private static double getFuncValue(int[][] linkMatrix) {

        double jSum = 0.0;

        for (int j = 0; j < ticVector.length; j++) {

            double iSum = 0.0;

            for (int i = 0; i < ticVector.length; i++) {
                iSum += linkMatrix[i][j] * ticVector[i] / lVector[i];
            }

            jSum += (getKCoefValue(linkMatrix) - ticVector[j] - iSum) * (getKCoefValue(linkMatrix) - ticVector[j] - iSum);

        }

        return jSum;
    }

    private static double getFuncValueMax(int[][] linkMatrix) {

        double sum = 0.0;

        for (int i = 0; i < linkMatrix.length; i++) {
            for (int j = 0; j < linkMatrix.length; j++) {
                sum += linkMatrix[i][j] * ticVector[i] / ticVector[j] / lVector[i];
            }
        }

        return sum;
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

    private static Map<String, Integer> getTic() throws SQLException {
        try (Connection connection = MyCrawler.connect()) {
            Map<String, Integer> tic;
            try (PreparedStatement pstmt1 = connection.prepareStatement(SQL_TIC,
                    Statement.RETURN_GENERATED_KEYS)) {
                try (ResultSet resultSet = pstmt1.executeQuery()) {
                    tic = new HashMap<>();
                    while (resultSet.next()) {
                        tic.put(resultSet.getString("seed"), resultSet.getInt("tic"));
                    }
                }
            }
            return tic;
        }
    }


    private static int[] getOnesVector(int[][] matrix) {
        int[] onesVector = new int[matrix.length];
        for (int i = 0; i < matrix.length; i++) {
            onesVector[i] = 0;
        }

        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix.length; j++) {
                if (matrix[i][j] == 1) {
                    onesVector[i]++;
                }
            }
        }

        return onesVector;
    }

    private static int calculateFunctional(int[][] matrix) {
        int sum = 0;

        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix.length; j++) {
                int value = matrix[i][j];
                for (int k = 1; k < matrix.length - j; k++) {
                    value = value * 10;
                }
                sum += value;
            }
        }

        return sum;
    }

    private static int[][] rebuildMatrix(int[][] matrix) {

        int[][] buildedMatrix = new int[matrix.length][matrix.length];
        int[][] optimizedMatrix = new int[matrix.length][matrix.length];

        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix.length; j++) {
                buildedMatrix[i][j] = matrix[i][j];
                optimizedMatrix[i][j] = matrix[i][j];
            }
        }

        for (int i = 0; i < matrix.length; i++) {
            System.out.println(i);
            doBinaryPermutation(new int[matrix.length], matrix.length, countOnes(buildedMatrix[i]),
                    buildedMatrix, optimizedMatrix, i);

        }

        return optimizedMatrix;
    }

    private static void doBinaryPermutation(int[] line, int size, int onesNum,
                                            int[][] buildedMatrix, int[][] optimizedMatrix, int rowNum) {
        //System.out.println(permutationCounter++);
        if (size == 0) {
            if (countOnes(line) == onesNum) {
                for (int j = 0; j < line.length; j++) {
                    buildedMatrix[rowNum][j] = line[j];
                }
                doCalcluation(buildedMatrix, optimizedMatrix);
            }
        }
        else {
            if (countOnes(line) == onesNum) {
                int[] fullLine = new int[line.length];
                for (int i = 0; i < line.length; i++) {
                    fullLine[i] = line[i];
                }
                for (int i = line.length - size; i < line.length; i++) {
                    fullLine[i] = 0;
                }
                doBinaryPermutation(fullLine, 0, onesNum, buildedMatrix, optimizedMatrix, rowNum);
            } else {
                int[] newLineZero = new int[line.length];
                for (int j = 0; j < line.length; j++) {
                    newLineZero[j] = line[j];
                }
                newLineZero[newLineZero.length - size] = 0;
                doBinaryPermutation(newLineZero, size - 1, onesNum, buildedMatrix, optimizedMatrix, rowNum);
                int[] newLineOne = new int[line.length];
                for (int j = 0; j < line.length; j++) {
                    newLineOne[j] = line[j];
                }

                newLineOne[newLineOne.length - size] = 1;
                doBinaryPermutation(newLineOne, size - 1, onesNum, buildedMatrix, optimizedMatrix, rowNum);
            }
        }
    }

    private static void doCalcluation(int[][] buildedMatrix, int[][] optimizedMatrix) {
        if (getFuncValueMax(buildedMatrix) > getFuncValueMax(optimizedMatrix) && isDiagonalEmpty(buildedMatrix)) {
            for (int i = 0; i < buildedMatrix.length; i++) {
                for (int j = 0; j < buildedMatrix.length; j++)
                    optimizedMatrix[i][j] = buildedMatrix[i][j];
            }
        } else {
            for (int i = 0; i < buildedMatrix.length; i++) {
                for (int j = 0; j < buildedMatrix.length; j++)
                    buildedMatrix[i][j] = optimizedMatrix[i][j];
            }
        }
    }

    private static boolean isDiagonalEmpty(int[][] matrix) {
        boolean flag = true;
        for (int j = 0; j < matrix.length; j++) {
            if (matrix[j][j] != 0)
                flag = false;
        }
        return flag;
    }

    private static int countOnes(int[] line) {
        int counter = 0;

        for (int i = 0; i < line.length; i++) {
            if (line[i] == 1) {
                counter++;
            }
        }
        return counter;
    }

    private static int[][] parseResult(String result, int size) {
        int[][] parsedMatrix = new int[size][size];
        for (int i=0; i < size; i++) {

        }
        return parsedMatrix;
    }

    private static double getDeviation (int[][] real, int[][] opt) {
        return getFuncValueMax(opt) / getFuncValueMax(real);
    }

}
