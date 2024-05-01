package krusty;

import spark.Request;
import spark.Response;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static krusty.Jsonizer.toJson;

public class Database {
	/**
	 * Modify it to fit your environment and then use this string when connecting to your database!
	 */

	 //TO DO

	// For use with MySQL or PostgreSQL
	private static final String jdbcUsername = "hbg07";
	private static final String jdbcPassword = "sbz864po";
	private static final String jdbcString = "jdbc:mysql://puccini.cs.lth.se/hbg07?user="+jdbcUsername+"&password=" +jdbcPassword;;


	private static final String Customersdef = "INSERT INTO Customer(customerName, address) VALUES" + 
	"   ('Finkakor AB', 'Helsingborg'), " +
    "   ('Småbröd AB', 'Malmö')," +
    "   ('Kaffebröd AB', 'Landskrona')," + 
    "   ('Bjudkakor AB', 'Ystad')," +
    "   ('Kalaskakor AB', 'Trelleborg')," + 
    "   ('Partykakor AB', 'Kristianstad')," +
    "   ('Gästkakor AB', 'Hässleholm')," +
    "   ('Skånekakor AB', 'Perstorp')," + 
 ";"
;


private static final String Cookiedef = "INSERT INTO Cookie (productName) VALUES" + 
"   ('Almond delight')," +
"   ('Amneris')," +
"   ('Berliner')," + 
"   ('Nut cookie')," +
"   ('Nut ring')," +
"    ('Tango')" +
";"
;

private static final String Ingredientsdef = "INSERT INTO Ingredients (ingredientName, QuantityInStorage, Unit) VALUES" +
    "   ('Bread crumbs', 500000, 'g'),"+
    "   ('Butter', 500000, 'g'),"+
    "   ('Chocolate', 500000, 'g'),"+
	"   ('Chopped almonds', 500000, 'g'),"+
    "   ('Cinnamon', 500000, 'g'),"+
    "   ('Egg whites', 500000, 'ml'),"+
	"   ('Eggs', 500000, 'g'),"+
    "   ('Fine-ground nuts', 500000, 'g'),"+
	"   ('Flour', 500000, 'g'),"+
    "   ('Ground, roasted nuts', 500000, 'g'),"+
    "   ('Icing sugar', 500000, 'g'),"+
    "   ('Marzipan', 500000, 'g'),"+
    "   ('Potato starch', 500000, 'g'),"+
    "   ('Roasted, chopped nuts', 500000, 'g'),"+
    "   ('Sodium bicarbonate', 500000, 'g'),"+
    "   ('Sugar', 500000, 'g'),"+
    "   ('Vanilla sugar', 500000, 'g'),"+
    "   ('Vanilla', 500000, 'g'),"+
    "   ('Wheat flour', 500000, 'g')"+
	";"
	;

	private static final String Recipesdef = "INSERT INTO Recipe(productName, ingredientName, Quantity) VALUES" +
	"VALUES( 'Almond delight', 'Butter', 400),"+
	"VALUES( 'Almond delight', 'Chopped almonds', 279),"+
	"VALUES( 'Almond delight', 'Cinnamon', 10),"+
	"VALUES( 'Almond delight', 'Flour', 400),"+
	"VALUES( 'Almond delight', 'Sugar', 270),"+
	"VALUES( 'Amneris', 'Marzipan' 750),"+
	"VALUES( 'Amneris', 'Butter' 250),"+
	"VALUES( 'Amneris', 'Eggs' 250),"+
	"VALUES( 'Amneris', 'Potato starch' 25),"+
	"VALUES( 'Amneris', 'Wheat flour' 25),"+
	"VALUES( 'Berliner', 'Butter', 250),"+
	"VALUES( 'Berliner', 'Chocolate', 50),"+
	"VALUES( 'Berliner', 'Eggs', 50),"+
	"VALUES( 'Berliner', 'Flour', 350),"+
	"VALUES( 'Berliner', 'Icing sugar', 100),"+
	"VALUES( 'Berliner', 'Vanilla sugar', 5),"+
	"VALUES( 'Nut cookie', 'Fine-ground nuts', 750),"+
	"VALUES( 'Nut cookie', 'Ground, roasted nuts', 625),"+
	"VALUES( 'Nut cookie', 'Bread crumbs', 125),"+
	"VALUES( 'Nut cookie', 'Sugar', 375),"+
	"VALUES( 'Nut cookie', 'Egg whites', 3.5),"+
	"VALUES( 'Nut cookie', 'Chocolate', 50),"+
	"VALUES( 'Nut Ring', 'Flour', 450),"+ 
	"VALUES( 'Nut Ring', 'Butter', 450),"+
	"VALUES( 'Nut Ring', 'Icing sugar', 190),"+
	"VALUES( 'Nut Ring', 'Roasted, chopped nuts', 225),"+
	"VALUES( 'Tango', 'Butter', 200),"+
	"VALUES( 'Tango', 'Flour', 300),"+
	"VALUES( 'Tango', 'Sodium bicarbonate', 4),"+
	"VALUES( 'Tango', 'Sugar', 250),"+
	"VALUES( 'Tango', 'Vanilla', 2)"+
	";"
	;   
   


	private Connection conn;
	public void connect() {
		try {
			conn = DriverManager.getConnection(jdbcString, jdbcUsername, jdbcPassword);

		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Connection failed");
			System.exit(1);
		}


	}

	public void close(){
		try {
			if (conn!= null){
				conn.close();
			}
		} catch (SQLException e) {
				e.printStackTrace();
		}


	}


	public String getCustomers(Request req, Response res) {
		try {
			String query = "SELECT customerName AS name, address as address FROM Customer";
			PreparedStatement conn = conn.prepareStatement(query);
			ResultSet resultSet = prepare.executeQuery();
			String json = Jsonizer.toJson(reusltSet, "customers");
			return json;

		}

		 catch (SQLxception e) {

			//Kanske onödigt???
					System.out.println("SQLMessage" + e.getMessage());
					System.out.println("SQLState:" + e.getSQLState());
					System.out.println("Error:" + e.getErrorCode());

		}

		return "{}";
	}

	public String getRawMaterials(Request req, Response res) {
		try {
		String query = "SELECT ingredientName AS Name, QuantityInStorage as Quantity, Unit FROM Ingredients";
		PreparedStatement conn = conn.prepareStatement(query);
		ResultSet resultSet = prepare.executeQuery();
		String json = Jsonizer.toJson(resultSet, "RawMaterials");
		return json;

		} catch (SQLException e) {
			System.out.println("SQLMessage" + e.getMessage());
			System.out.println("SQLState:" + e.getSQLState());
			System.out.println("Error:" + e.getErrorCode());
		}
		
		return "{}";
	}

	public String getCookies(Request req, Response res) {
		return "{\"cookies\":[]}";
	}

	//jag
	public String getRecipes(Request req, Response res) {


		try{

        String sql = "SELECT Recipe.productName, Recipe.ingredientName, Recipe.Quantity, Ingredients.QuantityInStorage, Ingredients.Unit from Recipe, Ingredients where Recipe.ingredientName= Ingredients.ingredientName";
		PreparedStatement ps = conn.prepareStatement(sql);
		resultSet rs = ps.executeQuery(sql);
		String result = krusty.Jsonizer.toJson(rs, "recipes");
        return result;
		}
		catch(SQLException e){
			e.printStackTrace();
		}
	}

	//jag

	public String getPallets(Request req, Response res) {
		String sql = "SELECT Pallet.palletID, Pallet.productName AS cookie, Pallet.ProductionDate, Orders.customerName AS customer, Pallet.blocked from Pallet inner join Orders on Orders.orderID = Pallet.orderID";
		String title = "pallets";
		StringBuilder sb = new StringBuilder();
	
		sb.append(sql);
	
		List<String> paramList = Arrays.asList("from", "to", "cookie", "blocked");
		HashMap<String, String> map = new HashMap<>();
	
		for (String param : paramList) {
			if (req.queryParams(param) != null) {
				map.put(param, req.queryParams(param));
			}
		}
	
		if (map.size() > 0) {
			sb.append(" WHERE");
		}
	
		int size = 1;
	
		for (Map.Entry<String, String> entry : map.entrySet()) {
			switch (entry.getKey()) {
				case "from":
					sb.append(" Pallet.ProductionDate >= ?");
					break;
				case "to":
					sb.append(" Pallet.ProductionDate <= ?");
					break;
				case "blocked":
					sb.append(" Pallet.blocked = ?");
					break;
				case "cookie":
					sb.append(" Pallet.productName = ?");
					break;
				default:
					break;
			}
			if (map.size() > size) {
				size++;
				sb.append(" AND");
			}
		}
	
		try (
			PreparedStatement stmt = conn.prepareStatement(sb.toString())
		) {
			int i = 1;
	
			for (Map.Entry<String, String> entry : map.entrySet()) {
				switch (entry.getKey()) {
					case "from":
						stmt.setDate(i, Date.valueOf(req.queryParams("from")));
						break;
					case "to":
						stmt.setDate(i, Date.valueOf(req.queryParams("to")));
						break;
					case "blocked":
						stmt.setString(i, req.queryParams("blocked"));
						break;
					case "cookie":
						stmt.setString(i, req.queryParams("cookie"));
						break;
					default:
						break;
				}
				i++;
			}
	
			try (
				ResultSet rs = stmt.executeQuery()
			) {
				String result = krusty.Jsonizer.toJson(rs, "pallets");
				System.out.println(result);
				return result;
			} catch (SQLException e) {
				e.printStackTrace();
				return "Error processing result set: " + e.getMessage();
			}
		} 
	}
	

	public String reset(Request req, Response res) {
	try{
		PreparedStatement pstmt = conn.prepareStatement("SET foreign_key_checks = 0;");
		pstmt.executeUpdate();

		truncateTable ("Recipe");
		truncateTable ("orderSpec");
		truncateTable ("Pallet");
		truncateTable ("Orders");
		truncateTable ("Ingredients");
		truncateTable ("Customer");

	 pstmt = conn.prepareStatement("SET foreign_key_checks = 1;");
		pstmt.executeUpdate();

		pstmt = conn.prepareStatement(Customersdef);
		pstmt.executeUpdate();

		pstmt = conn.prepareStatement(Cookiedef);
		pstmt.executeUpdate();

		pstmt = conn.prepareStatement(Ingredientsdef);
		pstmt.executeUpdate();

		pstmt = conn.prepareStatement(Recipesdef);
		pstmt.executeUpdate();



	}
	catch (SQLException e){
		System.out.println("SQLMessage" + e.getMessage());
		System.out.println("SQLState:" + e.getSQLState());
		System.out.println("Error:" + e.getErrorCode());

	}
		
		return "{}";
	}

	public String createPallet(Request req, Response res) {
		  
            String cookie = req.queryParams("cookie");
            String createPallet = "INSERT INTO Pallet (blocked, ProductionDate,productName) VALUES (no, NOW(),?)";
            String updateIngredients = "UPDATE Ingredients" + 
            "SET QuantityInStorage= QuantityInStorage - IFNULL("+ 
            "SELECT 54* Quantity" + 
            "FROM Recipe" + 
            "WHERE productName = ?" + 
            "And Ingredients.name = Recipe.ingredientName" + 
            "),0"+ 
            ");";

            conn.setAutoCommit(false); 

            try(
                PreparedStatement ps = conn.prepareStatement(createPallet);
                PreparedStatement ps2 = conn.prepareStatement(updateIngredients)){
                    ps.setString(1,selectedCookie);
                    int palletCreated = ps.executeUpdate();
                    if(palletCreated == 0) {
						return "{\"status\": \"unknown cookie\"}";
					};

                    ps.setString(1, selectedCookie);
                    int ingredientUpdate = psUpdate.executeUpdate();

                    if (palletCreated >0 && ingredientUpdate >0){
                        conn.commit();
                        return "{\"status\": \"id " + getPalletid() + "\"}"; 
                    }
                    else{
                        conn.rollback();
                    }

                } catch (SQLException e ){
                    conn.rollback();
                    e.printStackTrace();
                    return "{\"status\": \"error\"}";
                }
        
        return "{}";
	}

	//Privat hjälpmetod som utför trunkering av tabeller. Används i reset
	private void truncateTable (String table){
		String truncate = "TRUNCATE TABLE " + table;
		PReparedStatement pstmt = conn.prepareStatement(truncate);
		pstmt.executeUpdate();


	}

}
