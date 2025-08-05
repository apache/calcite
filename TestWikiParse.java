import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class TestWikiParse {
    public static void main(String[] args) throws Exception {
        String url = "https://en.wikipedia.org/w/index.php?title=List_of_states_and_territories_of_the_United_States&oldid=713721917";
        Document doc = Jsoup.connect(url).get();
        
        // Try the selector from wiki.json
        Elements tables = doc.select("#mw-content-text > table.wikitable.sortable");
        System.out.println("Found " + tables.size() + " tables with selector");
        
        for (int i = 0; i < tables.size(); i++) {
            Element table = tables.get(i);
            Elements headers = table.select("th");
            System.out.println("\nTable " + i + " headers:");
            for (Element th : headers) {
                System.out.println("  - " + th.text());
            }
            
            // Check first row
            Elements rows = table.select("tr");
            if (rows.size() > 1) {
                Element firstDataRow = rows.get(1);
                Elements cells = firstDataRow.select("td");
                System.out.println("First row has " + cells.size() + " cells");
            }
        }
    }
}
