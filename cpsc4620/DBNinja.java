package cpsc4620;

import java.io.IOException;
import java.sql.*;
import java.util.*;

/*
 * This file is where you will implement the methods needed to support this application.
 * You will write the code to retrieve and save information to the database and use that
 * information to build the various objects required by the applicaiton.
 * 
 * The class has several hard coded static variables used for the connection, you will need to
 * change those to your connection information
 * 
 * This class also has static string variables for pickup, delivery and dine-in. 
 * DO NOT change these constant values.
 * 
 * You can add any helper methods you need, but you must implement all the methods
 * in this class and use them to complete the project.  The autograder will rely on
 * these methods being implemented, so do not delete them or alter their method
 * signatures.
 * 
 * Make sure you properly open and close your DB connections in any method that
 * requires access to the DB.
 * Use the connect_to_db below to open your connection in DBConnector.
 * What is opened must be closed!
 */

/*
 * A utility class to help add and retrieve information from the database
 */

public final class DBNinja {
	private static Connection conn;

	// DO NOT change these variables!
	public final static String pickup = "pickup";
	public final static String delivery = "delivery";
	public final static String dine_in = "dinein";

	public final static String size_s = "Small";
	public final static String size_m = "Medium";
	public final static String size_l = "Large";
	public final static String size_xl = "XLarge";

	public final static String crust_thin = "Thin";
	public final static String crust_orig = "Original";
	public final static String crust_pan = "Pan";
	public final static String crust_gf = "Gluten-Free";

	public enum order_state {
		PREPARED,
		DELIVERED,
		PICKEDUP
	}


	private static boolean connect_to_db() throws SQLException, IOException 
	{

		try {
			conn = DBConnector.make_connection();
			return true;
		} catch (SQLException e) {
			return false;
		} catch (IOException e) {
			return false;
		}

	}

	public static void addOrder(Order o) throws SQLException, IOException 
	{
		/*
		 * add code to add the order to the DB. Remember that we're not just
		 * adding the order to the order DB table, but we're also recording
		 * the necessary data for the delivery, dinein, pickup, pizzas, toppings
		 * on pizzas, order discounts and pizza discounts.
		 * 
		 * This is a KEY method as it must store all the data in the Order object
		 * in the database and make sure all the tables are correctly linked.
		 * 
		 * Remember, if the order is for Dine In, there is no customer...
		 * so the cusomter id coming from the Order object will be -1.
		 * 
		 */
	}
	
	public static int addPizza(java.util.Date d, int orderID, Pizza p) throws SQLException, IOException
	{
		/*
		 * Add the code needed to insert the pizza into into the database.
		 * Keep in mind you must also add the pizza discounts and toppings 
		 * associated with the pizza.
		 * 
		 * NOTE: there is a Date object passed into this method so that the Order
		 * and ALL its Pizzas can be assigned the same DTS.
		 * 
		 * This method returns the id of the pizza just added.
		 * 
		 */

		connect_to_db();

		try {
			String pizzaQuery = "INSERT INTO pizza (pizza_PizzaID, pizza_Size, pizza_CrustType, ordertable_OrderID, pizza_PizzaState, pizza_PizzaDate, pizza_CustPrice, pizza_BusPrice) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

			PreparedStatement ps5 = conn.prepareStatement(pizzaQuery);
			ps5.setInt(1, p.getPizzaID());
			ps5.setString(2, p.getSize());
			ps5.setString(3, p.getCrustType());
			ps5.setInt(4, orderID);
			ps5.setString(5, p.getPizzaState());
			ps5.setTimestamp(6, new java.sql.Timestamp(d.getTime()));
			ps5.setDouble(7, p.getCustPrice());
			ps5.setDouble(8, p.getBusPrice());

			ps5.executeUpdate();

			// TOPPINGS
			ArrayList<Topping> toppings = p.getToppings();

			for (Topping topping : toppings) {

				// 1. LINK pizza ↔ topping
				String linkQuery = "INSERT INTO pizza_topping (pizza_PizzaID, topping_TopID) VALUES (?, ?)";
				PreparedStatement psLink = conn.prepareStatement(linkQuery);
				psLink.setInt(1, p.getPizzaID());
				psLink.setInt(2, topping.getTopID());
				psLink.executeUpdate();

				// 2. UPDATE INVENTORY
				double amountUsed = 0;

				if (p.getSize().equals("S")) {
					amountUsed = topping.getSmallAMT();
				} else if (p.getSize().equals("M")) {
					amountUsed = topping.getMedAMT();
				} else if (p.getSize().equals("L")) {
					amountUsed = topping.getLgAMT();
				} else if (p.getSize().equals("XL")) {
					amountUsed = topping.getXLAMT();
				}

				double newInv = topping.getCurINVT() - amountUsed;

				String updateQuery = "UPDATE topping SET topping_CurINVT = ? WHERE topping_TopID = ?";
				PreparedStatement ps6 = conn.prepareStatement(updateQuery);
				ps6.setDouble(1, newInv);
				ps6.setInt(2, topping.getTopID());

				ps6.executeUpdate();
			}

			// apply pizza discounts
			ArrayList<Discount> pizzaDiscounts = p.getDiscounts();

			for (Discount disc : pizzaDiscounts) {
				String pdQuery = "INSERT INTO pizza_discount (pizza_PizzaID, discount_DiscountID) VALUES (?, ?)";
				PreparedStatement psPD = conn.prepareStatement(pdQuery);

				psPD.setInt(1, p.getPizzaID());
				psPD.setInt(2, disc.getDiscountID());

				psPD.executeUpdate();
			}

			return p.getPizzaID();
		
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			conn.close();
		}

		return -1;
	}
	
	public static int addCustomer(Customer c) throws SQLException, IOException
	 {
		/*
		 * This method adds a new customer to the database.
		 * 
		 */

		connect_to_db();
		
		try {

			String query = "INSERT INTO customer (customer_CustID, customer_FName, customer_LName, customer_PhoneNum) VALUES (?,?,? ,? );";
		
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setInt(1, c.getCustID());
			ps.setString(2, c.getFName());
			ps.setString(3, c.getLName());
			ps.setString(4, c.getPhone());

			int rowsAffected = ps.executeUpdate();

			if (rowsAffected > 0){
				return c.getCustID();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		conn.close();
		 return -1;
	}

	public static ArrayList<Order> getOrders(int status) throws SQLException, IOException {
		
	}
	
	public static Order getLastOrder() throws SQLException, IOException 
	{
		/*
		 * Query the database for the LAST order added
		 * then return an Order object for that order.
		 * NOTE...there will ALWAYS be a "last order"!
		 */

		connect_to_db();
		try {
			String queryID = "SELECT * FROM ordertable ORDER BY ordertable_OrderID DESC LIMIT 1;";
			PreparedStatement psID = conn.prepareStatement(queryID);

			ResultSet rset = psID.executeQuery();

			int currentID = -1;
			if (rset.next()){
				currentID = rset.getInt("ordertable_OrderID");
			}

			// int nextID = (currentID + 1);
			//o.setOrderID(nextID);
			
			// ok, now we have the current ID. next, want to make an object with this
			String query = "SELECT * FROM ordertable WHERE ordertable_OrderID =?;";
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setInt(1, currentID);

			ResultSet rset2 = ps.executeQuery();

			if (rset2.next()){
				int orderID = rset2.getInt("ordertable_OrderID");
				int custID = rset2.getInt("customer_CustID");
				String orderType = rset2.getString("ordertable_OrderType");
				String Date = rset2.getString("ordertable_OrderDateTime");
				double custPrice = rset2.getDouble("ordertable_CustPrice");
				double busPrice = rset2.getDouble("ordertable_BusPrice");
				boolean iscomplete = rset2.getBoolean("ordertable_IsComplete");
				
				// create order object and then return object
				Order lastOrder = new Order(orderID, custID, orderType, Date, custPrice, busPrice, iscomplete);
				return lastOrder;

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			conn.close();
		}
		return null;
		
	}

	public static ArrayList<Order> getOrdersByDate(String date) throws SQLException, IOException
	 {
		/*
		 * Query the database for ALL the orders placed on a specific date
		 * and return a list of those orders.
		 *  
		 */

		connect_to_db();
		ArrayList<Order> orders = new ArrayList<>();

		try {
			String query = "SELECT * FROM ordertable WHERE DATE(ordertable_OrderDateTime) = ?;";
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setString(1, date);
			ResultSet rset = ps.executeQuery();

			while (rset.next()){
				int orderID = rset.getInt("ordertable_OrderID");
				int custID = rset.getInt("customer_CustID");
				String orderType = rset.getString("ordertable_OrderType");
				String Date = rset.getString("ordertable_OrderDateTime");
				double custPrice = rset.getDouble("ordertable_CustPrice");
				double busPrice = rset.getDouble("ordertable_BusPrice");
				boolean iscomplete = rset.getBoolean("ordertable_IsComplete");
				
				// create customer object and then return object
				Order order = new Order(orderID, custID, orderType, Date, custPrice, busPrice, iscomplete);
				orders.add(order);
			
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		finally {
        	conn.close();
   		}
		
		return orders;
	}
		
	public static ArrayList<Discount> getDiscountList() throws SQLException, IOException 
	{
		/* 
		 * Query the database for all the available discounts and 
		 * return them in an arrayList of discounts ordered by discount name.
		 * 
		*/

		connect_to_db();
		ArrayList<Discount> discounts = new ArrayList<>();

		try {
			String query = "SELECT * FROM discount ORDER BY discount_DiscountName;";
			PreparedStatement ps = conn.prepareStatement(query);
			ResultSet rset = ps.executeQuery();

			while (rset.next()){
				int id = rset.getInt("discount_DiscountID");
				String discountName = rset.getString("discount_DiscountName");
				double amount = rset.getDouble("discount_Amount");
				boolean isPercent = rset.getBoolean("discount_IsPercent");
				
				// create customer object and then return object
				Discount discount = new Discount(id, discountName, amount, isPercent);
				discounts.add(discount);
			
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		finally {
        	conn.close();
   		}

		return discounts;
	}

	public static Discount findDiscountByName(String name) throws SQLException, IOException 
	{
		/*
		 * Query the database for a discount using it's name.
		 * If found, then return an OrderDiscount object for the discount.
		 * If it's not found....then return null
		 *  
		 */

		connect_to_db();
		try {
			String query = "SELECT * FROM discount WHERE discount_DiscountName =?;";
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setString(1, name);

			ResultSet rset = ps.executeQuery();

			if (rset.next()){
				// extract data from the customer row
				int id = rset.getInt("discount_DiscountID");
				String discountName = rset.getString("discount_DiscountName");
				double amount = rset.getDouble("discount_Amount");
				boolean isPercent = rset.getBoolean("discount_IsPercent");
				
				// create customer object and then return object
				Discount foundDiscount = new Discount(id, discountName, amount, isPercent);
				return foundDiscount;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		conn.close();

		 return null;
	}


	public static ArrayList<Customer> getCustomerList() throws SQLException, IOException 
	{
		connect_to_db();
		
		ArrayList<Customer> customers = new ArrayList<Customer>();
		
		String query = "SELECT * FROM customer ORDER BY customer_LName, customer_FName, customer_PhoneNum";
		Statement stmt = conn.createStatement();
		ResultSet rset = stmt.executeQuery(query);
		
		while(rset.next()) {
			Customer c = new Customer(
				rset.getInt("customer_CustID"),
				rset.getString("customer_FName"),
				rset.getString("customer_LName"),
				rset.getString("customer_PhoneNum")
			);
			customers.add(c);
		}
		
		conn.close();
		return customers;
	}

	public static Customer findCustomerByPhone(String phoneNumber) throws SQLException, IOException
	{
		/*
		 * Query the database for a customer using a phone number.
		 * If found, then return a Customer object for the customer.
		 * If it's not found....then return null
		 *  
		 */

		connect_to_db();
		try {
			String query = "SELECT * FROM customer WHERE customer_PhoneNum =?;";
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setString(1, phoneNumber);

			ResultSet rset = ps.executeQuery();

			if (rset.next()){
				// extract data from the customer row
				int id = rset.getInt("customer_CustID");
				String fname = rset.getString("customer_Fname");
				String lname = rset.getString("customer_Lname");
				String phone = rset.getString("customer_PhoneNum");
				
				// create customer object and then return object
				Customer foundCustomer = new Customer(id, fname, lname, phone);
				return foundCustomer;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		conn.close();
		return null;
}

	public static String getCustomerName(int CustID) throws SQLException, IOException 
	{
		/*
		 * COMPLETED...WORKING Example!
		 * 
		 * This is a helper method to fetch and format the name of a customer
		 * based on a customer ID. This is an example of how to interact with
		 * your database from Java.  
		 * 
		 * Notice how the connection to the DB made at the start of the 
		 *
		 */

		 connect_to_db();

		/* 
		 * an example query using a constructed string...
		 * remember, this style of query construction could be subject to sql injection attacks!
		 * 
		 */
		String cname1 = "";
		String cname2 = "";
		String query = "Select customer_FName, customer_LName From customer WHERE customer_CustID=" + CustID + ";";
		Statement stmt = conn.createStatement();
		ResultSet rset = stmt.executeQuery(query);
		
		while(rset.next())
		{
			cname1 = rset.getString(1) + " " + rset.getString(2); 
		}

		/* 
		* an BETTER example of the same query using a prepared statement...
		* with exception handling
		* 
		*/
		try {
			PreparedStatement os;
			ResultSet rset2;
			String query2;
			query2 = "Select customer_FName, customer_LName From customer WHERE customer_CustID=?;";
			os = conn.prepareStatement(query2);
			os.setInt(1, CustID);
			rset2 = os.executeQuery();
			while(rset2.next())
			{
				cname2 = rset2.getString("customer_FName") + " " + rset2.getString("customer_LName"); // note the use of field names in the getSting methods
			}
		} catch (SQLException e) {
			e.printStackTrace();
			// process the error or re-raise the exception to a higher level
		}

		conn.close();

		return cname1;
		// OR
		// return cname2;

	}


	public static ArrayList<Topping> getToppingList() throws SQLException, IOException 
	{
		/*
		 * Query the database for the aviable toppings and 
		 * return an arrayList of all the available toppings. 
		 * Don't forget to order the data coming from the database appropriately.
		 * 
		 */
		return null;
	}

	public static Topping findToppingByName(String name) throws SQLException, IOException 
	{
		connect_to_db();

		try {
			String query = "SELECT * FROM topping WHERE topping_TopName = ?";
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setString(1, name);

			ResultSet rset = ps.executeQuery();

			if (rset.next()) {
				int id = rset.getInt("topping_TopID");
				String toppingName = rset.getString("topping_TopName");
				double small = rset.getDouble("topping_SmallAMT");
				double med = rset.getDouble("topping_MedAMT");
				double lg = rset.getDouble("topping_LgAMT");
				double xl = rset.getDouble("topping_XLAMT");
				double custPrice = rset.getDouble("topping_CustPrice");
				double busPrice = rset.getDouble("topping_BusPrice");
				int minINVT = rset.getInt("topping_MinINVT");
				int curINVT = rset.getInt("topping_CurINVT");

				Topping topping = new Topping(id, toppingName, small, med, lg, xl, custPrice, busPrice, minINVT, curINVT);
				
				conn.close();
				return topping;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		conn.close();
		return null;
	}

	public static ArrayList<Topping> getToppingsOnPizza(Pizza p) throws SQLException, IOException 
	/* 
		* This method builds an ArrayList of the toppings ON a pizza.
		* The list can then be added to the Pizza object elsewhere in the
		*/
	{
		connect_to_db();
		
		ArrayList<Topping> toppings = new ArrayList<Topping>();

		int pizzaID = p.getPizzaID();
		
		String query = "SELECT t.*, pt.pizza_topping_IsDouble FROM topping t JOIN pizza_topping pt ON t.topping_TopID = pt.topping_TopID WHERE pt.pizza_PizzaID = ?;";
		PreparedStatement ps = conn.prepareStatement(query);
		ps.setInt(1, pizzaID);
		ResultSet rset = ps.executeQuery();
		
		while (rset.next()) {
			Topping topping = new Topping(
					rset.getInt("topping_TopID"),
					rset.getString("topping_TopName"),
					rset.getDouble("topping_SmallAMT"),
					rset.getDouble("topping_MedAMT"),
					rset.getDouble("topping_LgAMT"),
					rset.getDouble("topping_XLAMT"),
					rset.getDouble("topping_CustPrice"),
					rset.getDouble("topping_BusPrice"),
					rset.getInt("topping_MinINVT"),
					rset.getInt("topping_CurINVT")
			);
			topping.setDoubled(rset.getBoolean("pizza_topping_IsDouble"));
			toppings.add(topping);
		}
		
		conn.close();
		return toppings;
	}

	public static void addToInventory(int toppingID, double quantity) throws SQLException, IOException 
	{
		/*
		 * Updates the quantity of the topping in the database by the amount specified.
		 * 
		 * */
	}
	
	
	public static ArrayList<Pizza> getPizzas(Order o) throws SQLException, IOException 
	{
		/*
		 * Build an ArrayList of all the Pizzas associated with the Order.
		 * 
		 */
		return null;
	}

	public static ArrayList<Discount> getDiscounts(Order o) throws SQLException, IOException 
	{
		/* 
		 * Build an array list of all the Discounts associted with the Order.
		 * 
		 */

		return null;
	}

	public static ArrayList<Discount> getDiscounts(Pizza p) throws SQLException, IOException 
	{
		/* 
		 * Build an array list of all the Discounts associted with the Pizza.
		 * 
		 */
	
		return null;
	}

	public static double getBaseCustPrice(String size, String crust) throws SQLException, IOException 
	{
		/* 
		 * Query the database fro the base customer price for that size and crust pizza.
		 * 
		*/
		return 0.0;
	}

	public static double getBaseBusPrice(String size, String crust) throws SQLException, IOException 
	{
		/* 
		 * Query the database fro the base business price for that size and crust pizza.
		 * 
		*/
		return 0.0;
	}

	
	public static void printToppingReport() throws SQLException, IOException
	{
		/*
		 * Prints the ToppingPopularity view. Remember that this view
		 * needs to exist in your DB, so be sure you've run your createViews.sql
		 * files on your testing DB if you haven't already.
		 * 
		 * The result should be readable and sorted as indicated in the prompt.
		 * 
		 * HINT: You need to match the expected output EXACTLY....I would suggest
		 * you look at the printf method (rather that the simple print of println).
		 * It operates the same in Java as it does in C and will make your code
		 * better.
		 * 
		 */
	}
	
	public static void printProfitByPizzaReport() throws SQLException, IOException 
	{
		/*
		 * Prints the ProfitByPizza view. Remember that this view
		 * needs to exist in your DB, so be sure you've run your createViews.sql
		 * files on your testing DB if you haven't already.
		 * 
		 * The result should be readable and sorted as indicated in the prompt.
		 * 
		 * HINT: You need to match the expected output EXACTLY....I would suggest
		 * you look at the printf method (rather that the simple print of println).
		 * It operates the same in Java as it does in C and will make your code
		 * better.
		 * 
		 */
	}
	
	public static void printProfitByOrderTypeReport() throws SQLException, IOException
	{
		/*
		 * Prints the ProfitByOrderType view. Remember that this view
		 * needs to exist in your DB, so be sure you've run your createViews.sql
		 * files on your testing DB if you haven't already.
		 * 
		 * The result should be readable and sorted as indicated in the prompt.
		 *
		 * HINT: You need to match the expected output EXACTLY....I would suggest
		 * you look at the printf method (rather that the simple print of println).
		 * It operates the same in Java as it does in C and will make your code
		 * better.
		 * 
		 */
	}
	
	
	
	/*
	 * These private methods help get the individual components of an SQL datetime object. 
	 * You're welcome to keep them or remove them....but they are usefull!
	 */
	private static int getYear(String date)// assumes date format 'YYYY-MM-DD HH:mm:ss'
	{
		return Integer.parseInt(date.substring(0,4));
	}
	private static int getMonth(String date)// assumes date format 'YYYY-MM-DD HH:mm:ss'
	{
		return Integer.parseInt(date.substring(5, 7));
	}
	private static int getDay(String date)// assumes date format 'YYYY-MM-DD HH:mm:ss'
	{
		return Integer.parseInt(date.substring(8, 10));
	}

	public static boolean checkDate(int year, int month, int day, String dateOfOrder)
	{
		if(getYear(dateOfOrder) > year)
			return true;
		else if(getYear(dateOfOrder) < year)
			return false;
		else
		{
			if(getMonth(dateOfOrder) > month)
				return true;
			else if(getMonth(dateOfOrder) < month)
				return false;
			else
			{
				if(getDay(dateOfOrder) >= day)
					return true;
				else
					return false;
			}
		}
	}


}
