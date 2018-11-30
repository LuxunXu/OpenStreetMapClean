package edu.ucr.cs.cs226.group2;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Experiment {

    public static void main(String[] args) {
        String temp = "<node id=\"5517856636\" lat=\"34.3113622\" lon=\"-116.9123841\" version=\"1\" timestamp=\"2018-03-31T06:41:58Z\" changeset=\"57680554\" uid=\"1239795\" user=\"release_candidate\"> " +
                "    <tag k=\"ele\" v=\"2340\"/> " +
                "    <tag k=\"natural\" v=\"peak\"/> " +
                "    <tag k=\"source\" v=\"USGS\"/> " +
                "  </node>";

        Document d = Jsoup.parse(temp);
        Elements inputs = d.select("node");
        for (Element el : inputs) {
            Attributes attrs = el.attributes();
            //System.out.print("ELEMENT: " + el.tagName());
            for (Attribute attr : attrs) {
                //System.out.print(" " + attr.getKey() + "=" + attr.getValue());
                if (attr.getKey().equals("id")) {
                    System.out.println(attr.getValue());
                } else {
                    System.out.println(attr.getKey() + "=\"" + attr.getValue() + "\"");
                }
            }
        }

        inputs = d.select("tag");
        for (Element el : inputs) {
            Attributes attrs = el.attributes();
            //System.out.print("ELEMENT: " + el.tagName());
            for (Attribute attr : attrs) {
                //System.out.print(" " + attr.getKey() + "=" + attr.getValue());
                if (attr.getKey().equals("id")) {
                    System.out.println(attr.getValue());
                } else {
                    System.out.println(attr.getKey() + "=\"" + attr.getValue() + "\"");
                }
            }
        }
    }
}
