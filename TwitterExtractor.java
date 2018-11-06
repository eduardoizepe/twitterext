package br.com.izepe.twitterext;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import twitter4j.GeoLocation;
import twitter4j.GeoQuery;
import twitter4j.IDs;
import twitter4j.JSONArray;
import twitter4j.JSONObject;
import twitter4j.Paging;
import twitter4j.Place;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;

public class TwitterExtractor {
	
	final static int TOTAL_PAGINAS = 10;
	final static String PATH_ENTRADA = "/usr/local/hadoop/programs/dados/";

	public static void main(String[] args) {
 				
		if (args.length < 1) {
            System.out.println("Usage: java br.com.izepe.twitterext.TwitterExtractor [username]");
            System.exit(-1);
        }
		
		Twitter twitter = new TwitterFactory().getInstance();
        try {
        	
        	long cursor = -1;
            IDs ids;
                       
            System.out.println("Listing followers's for " + args[0]);
    		String filename = PATH_ENTRADA + args[0] + ".json";
    		FileOutputStream fos = new FileOutputStream(filename);
    		OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
    		BufferedWriter bw = new BufferedWriter(osw);	
            do {
                ids = twitter.getFollowersIDs(args[0], cursor);
                for (long id : ids.getIDs()) {
                	User user = twitter.showUser(id);
                	if(userSelected(user)) {
                		System.out.println("User Selecionado: " + user.getScreenName() + " (" + user.getLocation() + ") (" + user.getEmail() + ")");
                		JSONObject jo = new JSONObject();
                		jo.put("user", user.getScreenName());
                		jo.put("location", user.getLocation());
                		if(user.getEmail() != null && !user.getEmail().equals(""))
                			jo.put("email", user.getEmail());
                		jo.put("name", user.getName());
                		getTimeLine(twitter,  user.getScreenName(), jo);
                		bw.write(jo.toString() + "\n");
                		bw.flush();
                		Thread.sleep(5000); //Sleep 5 segundos para tentar não estourar o limite de chamadas da API
                	}
                }
            } while ((cursor = ids.getNextCursor()) != 0);
             
            bw.close();
            osw.close();
            fos.close();
            System.out.println("Arquivo gerado com sucesso: " + filename);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get informaction from twitter: " + te.getMessage());
        } catch (Exception en) {
        	en.printStackTrace();
        	System.out.println("Generic Excepetion: " + en.getMessage());
        }
	}
	
	private static void getTimeLine(Twitter twitter, String user, JSONObject jo) throws TwitterException, Exception {
	 JSONArray jsonArr = new JSONArray();
	 int qtdPostagens=0;
	 
	 try {
		 Paging page = new Paging(1);
		 List<Status> statuses;
		 boolean flgSta = true;
         
         do {
        	 statuses = twitter.getUserTimeline(user, page);
        	 for (Status status : statuses) {
        		 if(status.getGeoLocation() != null && flgSta == true) {
        			 flgSta = false;
        			 GeoQuery query = new GeoQuery(new GeoLocation(status.getGeoLocation().getLatitude(), status.getGeoLocation().getLongitude()));
        			 ResponseList<Place> places = twitter.reverseGeoCode(query);
        			 if (places.size() != 0) {
        				Place place = places.get(0);
         				jo.put("location", place.getFullName());
         				Thread.sleep(5000); //Sleep 5 segundos para tentar não estourar o limite de chamadas da API
        			 }
        		 } 
        		 qtdPostagens++;
        		 jsonArr.put(status.getText());
        	 }
        	 page.setPage(page.getPage() + 1);
         } while (statuses.size() > 0 && page.getPage() <= TOTAL_PAGINAS);
 		
	 } catch (TwitterException te) {
         te.printStackTrace();
         System.out.println("Failed to get timeline: " + te.getMessage());
         throw new TwitterException(te);
     }
	 jo.put("qtdPostagens", qtdPostagens);
	 jo.put("postagens", jsonArr);
	 return;
	
	}
	
	private static boolean userSelected(User user) {
		if(user.isProtected()) return false;                             //SE O USUARIO NAO PERMTIR VER SUAS PUBLICACOES
		if(user.getLocation() == null || user.getLocation().equals("")) return false; //SE O USUARIO NAO POSSUI LOCALIZACAO NO PERFIL
		return true;
	}

}
