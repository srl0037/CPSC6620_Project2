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


	public static void completeOrder(int orderID, order_state newState) throws SQLException, IOException {
    connect_to_db();

    try {

        // PREPARED → mark order + pizzas complete
        if (newState == order_state.PREPARED) {

            String orderQuery = "UPDATE ordertable SET ordertable_IsComplete = 1 WHERE ordertable_OrderID = ?";
            PreparedStatement ps1 = conn.prepareStatement(orderQuery);
            ps1.setInt(1, orderID);
            ps1.executeUpdate();

            String pizzaQuery = "UPDATE pizza SET pizza_PizzaState = 'Completed' WHERE ordertable_OrderID = ?";
            PreparedStatement ps2 = conn.prepareStatement(pizzaQuery);
            ps2.setInt(1, orderID);
            ps2.executeUpdate();
        }

        // DELIVERED → update delivery table
        else if (newState == order_state.DELIVERED) {

            String deliveryQuery = "UPDATE delivery SET delivery_IsDelivered = 1 WHERE ordertable_OrderID = ?";
            PreparedStatement ps = conn.prepareStatement(deliveryQuery);
            ps.setInt(1, orderID);
            ps.executeUpdate();
        }

        // PICKEDUP → update pickup table
        else if (newState == order_state.PICKEDUP) {

            String pickupQuery = "UPDATE pickup SET pickup_IsPickedUp = 1 WHERE ordertable_OrderID = ?";
            PreparedStatement ps = conn.prepareStatement(pickupQuery);
            ps.setInt(1, orderID);
            ps.executeUpdate();
        }

    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        conn.close();
    }
}

	public static void addOrder(Order o) throws SQLException, IOException 
	{
		connect_to_db();

		try {

			// gen order ID
			String queryID = "SELECT * FROM ordertable ORDER BY ordertable_OrderID DESC LIMIT 1;";
			PreparedStatement psID = conn.prepareStatement(queryID);
			ResultSet rset = psID.executeQuery();

			int currentID = -1;
			if (rset.next()){
				currentID = rset.getInt("ordertable_OrderID");
			}

			int nextID = currentID + 1;
			o.setOrderID(nextID);

			// insert order
			String query = "INSERT INTO ordertable (ordertable_OrderID, customer_CustID, ordertable_OrderType, ordertable_OrderDateTime, ordertable_CustPrice, ordertable_BusPrice, ordertable_IsComplete) VALUES (?, ?, ?, ?, ?, ?, ?);";
			
			PreparedStatement ps = conn.prepareStatement(query);

			ps.setInt(1, o.getOrderID());

			if (o.getCustID() == -1) {
				ps.setNull(2, java.sql.Types.INTEGER);
			} else {
				ps.setInt(2, o.getCustID());
			}

			ps.setString(3, o.getOrderType());
			ps.setString(4, o.getDate());
			ps.setDouble(5, o.getCustPrice());
			ps.setDouble(6, o.getBusPrice());
			ps.setBoolean(7, o.getIsComplete());

			int rowsAffected = ps.executeUpdate();

			// order type
			if (rowsAffected > 0) {

				if (o instanceof DeliveryOrder) {
					DeliveryOrder d = (DeliveryOrder) o;

					String deliveryQuery = "INSERT INTO delivery (ordertable_OrderID, delivery_Address, delivery_IsDelivered) VALUES (?, ?, ?);";
					PreparedStatement ps2 = conn.prepareStatement(deliveryQuery);

					ps2.setInt(1, d.getOrderID());
					ps2.setString(2, d.getAddress());
					ps2.setBoolean(3, false);

					ps2.executeUpdate();
				}

				else if (o instanceof PickupOrder) {
					PickupOrder p = (PickupOrder) o;

					String pickupQuery = "INSERT INTO pickup (ordertable_OrderID, pickup_IsPickedUp) VALUES (?, ?);";
					PreparedStatement ps3 = conn.prepareStatement(pickupQuery);

					ps3.setInt(1, p.getOrderID());
					ps3.setBoolean(2, p.getIsPickedUp());

					ps3.executeUpdate();
				}

				else if (o instanceof DineinOrder) {
					DineinOrder dine = (DineinOrder) o;

					String dineInQuery = "INSERT INTO dinein (ordertable_OrderID, dinein_TableNum) VALUES (?, ?);";
					PreparedStatement ps4 = conn.prepareStatement(dineInQuery);

					ps4.setInt(1, dine.getOrderID());
					ps4.setInt(2, dine.getTableNum());

					ps4.executeUpdate();
				}
			}

			// order discount
			ArrayList<Discount> orderDiscounts = o.getDiscountList();

			for (Discount d : orderDiscounts) {
				String odQuery = "INSERT INTO order_discount (ordertable_OrderID, discount_DiscountID) VALUES (?, ?)";
				PreparedStatement psOD = conn.prepareStatement(odQuery);

				psOD.setInt(1, o.getOrderID());
				psOD.setInt(2, d.getDiscountID());

				psOD.executeUpdate();
			}


			// for every pizza in order
			ArrayList<Pizza> pizzaList = o.getPizzaList();

			for (Pizza pizza : pizzaList) {
				addPizza(new java.util.Date(), o.getOrderID(), pizza);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			conn.close();
		}
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
			String queryID = "SELECT * FROM pizza ORDER BY pizza_PizzaID DESC LIMIT 1;";
			PreparedStatement psID = conn.prepareStatement(queryID);
			ResultSet rset = psID.executeQuery();

			int currentID = -1;
			if (rset.next()) {
				currentID = rset.getInt("pizza_PizzaID");
			}

			int nextID = currentID + 1;
			p.setPizzaID(nextID);

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

			// toppings
			ArrayList<Topping> toppings = p.getToppings();

			for (Topping topping : toppings) {

				// link pizza and toppings
				String linkQuery = "INSERT INTO pizza_topping (pizza_PizzaID, topping_TopID, pizza_topping_IsDouble) VALUES (?, ?, ?)";
				PreparedStatement psLink = conn.prepareStatement(linkQuery);
				psLink.setInt(1, p.getPizzaID());
				psLink.setInt(2, topping.getTopID());
				psLink.setBoolean(3, topping.getDoubled());
				psLink.executeUpdate();

				// update inventory
				double amountUsed = 0;

				if (p.getSize().equals(DBNinja.size_s)) {
					amountUsed = topping.getSmallAMT();
				} else if (p.getSize().equals(DBNinja.size_m)) {
					amountUsed = topping.getMedAMT();
				} else if (p.getSize().equals(DBNinja.size_l)) {
					amountUsed = topping.getLgAMT();
				} else if (p.getSize().equals(DBNinja.size_xl)) {
					amountUsed = topping.getXLAMT();
				}

				if (topping.getDoubled()) {
    				amountUsed *= 2;
				}

				String updateQuery = "UPDATE topping SET topping_CurINVT = topping_CurINVT - ? WHERE topping_TopID = ?";
				PreparedStatement ps6 = conn.prepareStatement(updateQuery);
				ps6.setDouble(1, amountUsed);
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

	public static ArrayList<Order> getOrders(int status) throws SQLException, IOException 
		{

		connect_to_db();
		ArrayList<Order> orders = new ArrayList<>();

		try {
			String query;

			if (status == 1) {
				query = "SELECT * FROM ordertable WHERE ordertable_IsComplete = 0 ORDER BY ordertable_OrderID";
			} else if (status == 2) {
				query = "SELECT * FROM ordertable WHERE ordertable_IsComplete = 1 ORDER BY ordertable_OrderID";
			} else {
				query = "SELECT * FROM ordertable ORDER BY ordertable_OrderID";
			}

			PreparedStatement ps = conn.prepareStatement(query);
			ResultSet rset = ps.executeQuery();

			while (rset.next()) {

				int orderID = rset.getInt("ordertable_OrderID");

				int custID = rset.getInt("customer_CustID");
				if (rset.wasNull()) custID = -1;

				String orderType = rset.getString("ordertable_OrderType");
				String date = rset.getString("ordertable_OrderDateTime");
				double custPrice = rset.getDouble("ordertable_CustPrice");
				double busPrice = rset.getDouble("ordertable_BusPrice");
				boolean isComplete = rset.getBoolean("ordertable_IsComplete");

				Order order = new Order(orderID, custID, orderType, date, custPrice, busPrice, isComplete);

				//  use helper methods
				order.setDiscountList(getDiscounts(order));
				order.setPizzaList(getPizzas(order));

				orders.add(order);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			conn.close();
		}

		return orders;
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
				
				
				order.setDiscountList(getDiscounts(order));
				order.setPizzaList(getPizzas(order));
			
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
				String fname = rset.getString("customer_FName");
				String lname = rset.getString("customer_LName");
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
		connect_to_db();
		ArrayList<Topping> toppings = new ArrayList<>();

		try {
			String query = "SELECT * FROM topping WHERE topping_SmallAMT > 0 OR topping_MedAMT > 0 OR topping_LgAMT > 0 OR topping_XLAMT > 0 ORDER BY topping_TopName;";
			PreparedStatement ps = conn.prepareStatement(query);
			ResultSet rset = ps.executeQuery();

			while (rset.next()){
				int id = rset.getInt("topping_TopID");
				String name = rset.getString("topping_TopName");
				double small = rset.getDouble("topping_SmallAMT");
				double med = rset.getDouble("topping_MedAMT");
				double lg = rset.getDouble("topping_LgAMT");
				double xl = rset.getDouble("topping_XLAMT");
				double custPrice = rset.getDouble("topping_CustPrice");
				double busPrice = rset.getDouble("topping_BusPrice");
				int minINVT = rset.getInt("topping_MinINVT");
				int curINVT = rset.getInt("topping_CurINVT");


				Topping topping = new Topping(id, name, small, med, lg, xl, custPrice, busPrice, minINVT, curINVT);
				toppings.add(topping);				
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		conn.close();
		return toppings;
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
    connect_to_db();
    
    String query = "UPDATE topping SET topping_CurINVT = topping_CurINVT + ? WHERE topping_TopID = ?";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setDouble(1, quantity);
    stmt.setInt(2, toppingID);
    stmt.executeUpdate();
    
    conn.close();
}
	
	
	public static ArrayList<Pizza> getPizzas(Order o) throws SQLException, IOException 
{
    connect_to_db();
    
    ArrayList<Pizza> pizzas = new ArrayList<Pizza>();
    
    String query = "SELECT * FROM pizza WHERE ordertable_OrderID = ?";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setInt(1, o.getOrderID());
    ResultSet rset = stmt.executeQuery();
    
    while(rset.next()) {
        Pizza p = new Pizza(
            rset.getInt("pizza_PizzaID"),
            rset.getString("pizza_Size"),
            rset.getString("pizza_CrustType"),
            rset.getInt("ordertable_OrderID"),
            rset.getString("pizza_PizzaState"),
            rset.getString("pizza_PizzaDate"),
            rset.getDouble("pizza_CustPrice"),
            rset.getDouble("pizza_BusPrice")
        );
        
        // get toppings and discounts for each pizza

        // conn.close();
        p.setToppings(getToppingsOnPizza(p));
        p.setDiscounts(getDiscounts(p));
        // connect_to_db();

        
        pizzas.add(p);
    }
    
    conn.close();
    return pizzas;
}

	public static ArrayList<Discount> getDiscounts(Order o) throws SQLException, IOException 
{
    connect_to_db();
    
    ArrayList<Discount> discounts = new ArrayList<Discount>();
    
    String query = "SELECT d.* FROM discount d JOIN order_discount od ON d.discount_DiscountID = od.discount_DiscountID WHERE od.ordertable_OrderID = ?";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setInt(1, o.getOrderID());
    ResultSet rset = stmt.executeQuery();
    
    while(rset.next()) {
        Discount d = new Discount(
            rset.getInt("discount_DiscountID"),
            rset.getString("discount_DiscountName"),
            rset.getDouble("discount_Amount"),
            rset.getBoolean("discount_IsPercent")
        );
        discounts.add(d);
    }
    
    conn.close();
    return discounts;
}

	public static ArrayList<Discount> getDiscounts(Pizza p) throws SQLException, IOException 
{
    connect_to_db();
    
    ArrayList<Discount> discounts = new ArrayList<Discount>();
    
    String query = "SELECT d.* FROM discount d JOIN pizza_discount pd ON d.discount_DiscountID = pd.discount_DiscountID WHERE pd.pizza_PizzaID = ?";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setInt(1, p.getPizzaID());
    ResultSet rset = stmt.executeQuery();
    
    while(rset.next()) {
        Discount d = new Discount(
            rset.getInt("discount_DiscountID"),
            rset.getString("discount_DiscountName"),
            rset.getDouble("discount_Amount"),
            rset.getBoolean("discount_IsPercent")
        );
        discounts.add(d);
    }
    
    conn.close();
    return discounts;
}

	public static double getBaseCustPrice(String size, String crust) throws SQLException, IOException 
{
    connect_to_db();
    
    double price = 0.0;
    
    String query = "SELECT baseprice_CustPrice FROM baseprice WHERE baseprice_Size = ? AND baseprice_CrustType = ?";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, size);
    stmt.setString(2, crust);
    ResultSet rset = stmt.executeQuery();
    
    if(rset.next()) {
        price = rset.getDouble("baseprice_CustPrice");
    }
    
    conn.close();
    return price;
}

	public static double getBaseBusPrice(String size, String crust) throws SQLException, IOException 
{
    connect_to_db();
    
    double price = 0.0;
    
    String query = "SELECT baseprice_BusPrice FROM baseprice WHERE baseprice_Size = ? AND baseprice_CrustType = ?";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, size);
    stmt.setString(2, crust);
    ResultSet rset = stmt.executeQuery();
    
    if(rset.next()) {
        price = rset.getDouble("baseprice_BusPrice");
    }
    
    conn.close();
    return price;
}

	
	public static void printToppingReport() throws SQLException, IOException
{
    connect_to_db();
    
    String query = "SELECT * FROM toppingpopularity";
    Statement stmt = conn.createStatement();
    ResultSet rset = stmt.executeQuery(query);
    
    System.out.printf("%-30s %s%n", "Topping", "Topping Count");
    System.out.printf("%-30s %s%n", "-------", "-------------");
    
    while(rset.next()) {
        System.out.printf("%-30s %d%n", 
            rset.getString("Topping"), 
            rset.getInt("ToppingCount"));
    }
    
    conn.close();
}
	
	public static void printProfitByPizzaReport() throws SQLException, IOException 
{
    connect_to_db();
    
    String query = "SELECT * FROM profitbypizza";
    Statement stmt = conn.createStatement();
    ResultSet rset = stmt.executeQuery(query);
    
    System.out.printf("%-15s %-15s %-10s %s%n", "Pizza Size", "Pizza Crust", "Profit", "Last Order Date");
    System.out.printf("%-15s %-15s %-10s %s%n", "----------", "-----------", "------", "---------------");
    
    while(rset.next()) {
        System.out.printf("%-15s %-15s %-10.2f %s%n",
            rset.getString("Size"),
            rset.getString("Crust"),
            rset.getDouble("Profit"),
            rset.getString("OrderMonth"));
    }
    
    conn.close();
}
	
	public static void printProfitByOrderTypeReport() throws SQLException, IOException
{
    connect_to_db();
    
    String query = "SELECT * FROM profitbyordertype WHERE CustomerType != ''";
    Statement stmt = conn.createStatement();
    ResultSet rset = stmt.executeQuery(query);
    
    System.out.printf("%-15s %-15s %-20s %-20s %s%n", "Customer Type", "Order Month", "Total Order Price", "Total Order Cost", "Profit");
    System.out.printf("%-15s %-15s %-20s %-20s %s%n", "-------------", "-----------", "-----------------", "----------------", "------");
    
    double totalPrice = 0.0;
    double totalCost = 0.0;
    double totalProfit = 0.0;
    
    while(rset.next()) {
        double price = rset.getDouble("TotalOrderPrice");
        double cost = rset.getDouble("TotalOrderCost");
        double profit = rset.getDouble("Profit");
        
        totalPrice += price;
        totalCost += cost;
        totalProfit += profit;
        
        System.out.printf("%-15s %-15s %-20.2f %-20.2f %.2f%n",
            rset.getString("CustomerType"),
            rset.getString("OrderMonth"),
            price,
            cost,
            profit);
    }
    
    System.out.printf("%-15s %-15s %-20.2f %-20.2f %.2f%n",
        "", "Grand Total", totalPrice, totalCost, totalProfit);
    
    conn.close();
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