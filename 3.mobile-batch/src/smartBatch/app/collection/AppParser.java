package smartBatch.app.collection;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.lang.StringIndexOutOfBoundsException;

import smartBatch.app.model.AppModel;
import smartBatch.app.set.PackageSet;
import DB.DBConnection;

public class AppParser {
	DBConnection dbcon;
	String filtername;
	PackageSet Apps = new PackageSet();
	
	public AppParser(DBConnection dbcon, String filtername){
		this.dbcon = dbcon;
		this.filtername = filtername;
	}
	
	public Collection access() {
		Apps.setDbcon(dbcon);
		Collection AddingApps = new ArrayList<AppModel>();
		Collection NewApp = Apps.newPackageSet(filtername);
		
		if(NewApp != null) {
			Iterator it = NewApp.iterator();
			try {
				while(it.hasNext()) {
					AppModel App=(AppModel)it.next();

					String packagename = App.getPackagename();
					String type = "";
					String title = "";
					String provider = "";
					String site = "";
					int installs = 0;
					int smartid = App.getSmartid();
					int randomSleep = 0;
					//packagename=packagename.replaceAll("[^0-9a-zA-Z\\\\.]", "");
					//System.out.println(packagename+"||"+smartid);
					
					//Google App Play Store 사이트에서 가져옴, 한글 데이터를 위해 hl=ko 파라미터 추가
					URL url = new URL("https://play.google.com/store/apps/details?&hl=ko&id="+packagename);
					HttpURLConnection uc = (HttpURLConnection)url.openConnection();
					uc.connect();
					
					if (uc.getResponseCode() < 400){
						InputStream is = uc.getInputStream();
						BufferedReader br = new BufferedReader(new InputStreamReader(is,"utf-8"));
						String line = null;
						
						while ((line = br.readLine()) != null) {
							//System.out.println(line);
							if(title.equals("")&&line.contains("h1 class=\"AHFaub\" itemprop=\"name\"><span >")){
								try {
									title = line.substring(line.indexOf("h1 class=\"AHFaub\" itemprop=\"name\"><span >")+41);
									title = title.substring(0,title.indexOf("</span>"));
									if(title.length()-1 == title.indexOf("<")) {
										title = title.substring(0,title.length()-1);
										title = title.replace("&amp;","&");
									}
								} catch(StringIndexOutOfBoundsException e) {
									e.printStackTrace();
								}
							}
							if(provider.equals("")&line.contains("<div class=\"ZVWMWc\"><div class=\"i4sPve\">")){
								provider = line.substring(line.indexOf("https://play.google.com/store/apps/developer?id=")+48);
								provider = provider.substring(provider.indexOf("class=\"hrTbp R8zArc\">")+21);
								provider = provider.substring(0,provider.indexOf("</a>"));
							}
							if(type.equals("")&line.contains("<a itemprop=\"genre\" href=\"https://play.google.com/store/apps/category/")){
								type = line.substring(line.indexOf("https://play.google.com/store/apps/category/")+44);
								type = type.substring(type.indexOf("class=\"hrTbp R8zArc\">")+21);
								type = type.substring(0,type.indexOf("</"));
							}
							if(installs == 0 && line.contains("설치 수")){
								String install = line.substring(line.indexOf("설치 수</div><span class=\"htlgb\"><div><span class=\"htlgb\">")+55);							
								install = install.substring(0,install.indexOf("+"));								
								installs = Integer.parseInt(install.trim().replace(",",""));
							}
							if(line.contains("<time itemprop=\"datePublished\">")){
								String update_date = line.substring(line.indexOf("<time itemprop=\"datePublished\">")+21);
								update_date = update_date.substring(0,update_date.indexOf("<"));
								//System.out.println(update_date);
							}
							if(line.contains("웹사이트 방문")){
								site = line.substring(line.indexOf("<span class=\"htlgb\"><div><a href=\"")+34);
								site = site.substring(0,site.indexOf("\" class=\"hrTbp \">"));
								if(site.contains("nexon.co.kr")||site.contains("nexonmobile.com")){
									site = "nexon.com";
								}
							}
						}
						//System.out.println(type);
						//System.out.println(title);
						//System.out.println(provider);
						//System.out.println(update_date);
						//System.out.println(site);
						System.out.println("Search:"+packagename+"||"+smartid+"||"+type+"||"+provider+"||"+site+"||"+title+"||"+installs);
						if(!title.equals("")){
							if(site.equals("http://")){
								site = "";
							}
							site = site.toLowerCase();
							AppModel tg = new AppModel(packagename, smartid, type, provider, site, title, installs);
							AddingApps.add(tg);
						}
					}
					//google 접근시 블로킹 방지를 위해 random buffer를 사용하여 접근함
					Random rand = new Random(System.currentTimeMillis());
					randomSleep = Math.abs(rand.nextInt(10000)+20000);
					//System.out.println("sleep:"+randomSleep);
					//Thread.sleep(randomSleep);
				}
				//System.exit(0);
			} catch(Exception e){
				e.printStackTrace();
			}
		}
		return AddingApps;
	}
}
