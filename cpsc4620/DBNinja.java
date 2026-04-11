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

		return -1;
	}
	
	public static int addCustomer(Customer c) throws SQLException, IOException
	 {
		/*
		 * This method adds a new customer to the database.
		 * 
		 */

		 return -1;
	}

	public static void completeOrder(int OrderID, order_state newState ) throws SQLException, IOException
	{
		/*
		 * Mark that order as complete in the database.
		 * Note: if an order is complete, this means all the pizzas are complete as well.
		 * However, it does not mean that the order has been delivered or picked up!
		 *
		 * For newState = PREPARED: mark the order and all associated pizza's as completed
		 * For newState = DELIVERED: mark the delivery status
		 * FOR newState = PICKEDUP: mark the pickup status
		 * 
		 */

	}


	public static ArrayList<Order> getOrders(int status) throws SQLException, IOException
	 {
	/*
	 * Return an ArrayList of orders.
	 * 	status   == 1 => return a list of open (ie oder is not completed)
	 *           == 2 => return a list of completed orders (ie order is complete)
	 *           == 3 => return a list of all the orders
	 * Remember that in Java, we account for supertypes and subtypes
	 * which means that when we create an arrayList of orders, that really
	 * means we have an arrayList of dineinOrders, deliveryOrders, and pickupOrders.
	 *
	 * You must fully populate the Order object, this includes order discounts,
	 * and pizzas along with the toppings and discounts associated with them.
	 * 
	 * Don't forget to order the data according to their order sequence, ie, order 1, order 2, etc.
	 *
	 */
		return null;
	}
	
	public static Order getLastOrder() throws SQLException, IOException 
	{
		/*
		 * Query the database for the LAST order added
		 * then return an Order object for that order.
		 * NOTE...there will ALWAYS be a "last order"!
		 */
		 return null;
	}

	public static ArrayList<Order> getOrdersByDate(String date) throws SQLException, IOException
	 {
		/*
		 * Query the database for ALL the orders placed on a specific date
		 * and return a list of those orders.
		 *  
		 */
		 return null;
	}
		
	public static ArrayList<Discount> getDiscountList() throws SQLException, IOException 
	{
		/* 
		 * Query the database for all the available discounts and 
		 * return them in an arrayList of discounts ordered by discount name.
		 * 
		*/
		return null;
	}

	public static Discount findDiscountByName(String name) throws SQLException, IOException 
	{
		/*
		 * Query the database for a discount using it's name.
		 * If found, then return an OrderDiscount object for the discount.
		 * If it's not found....then return null
		 *  
		 */
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
    connect_to_db();
    
    Customer c = null;
    
    String query = "SELECT * FROM customer WHERE customer_PhoneNum = ?";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, phoneNumber);
    ResultSet rset = stmt.executeQuery();
    
    if(rset.next()) {
        c = new Customer(
            rset.getInt("customer_CustID"),
            rset.getString("customer_FName"),
            rset.getString("customer_LName"),
            rset.getString("customer_PhoneNum")
        );
    }
    
    conn.close();
    return c;
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
		/*
		 * Query the database for the topping using it's name.
		 * If found, then return a Topping object for the topping.
		 * If it's not found....then return null
		 *  
		 */
		 return null;
	}

	public static ArrayList<Topping> getToppingsOnPizza(Pizza p) throws SQLException, IOException 
	{
		/* 
		 * This method builds an ArrayList of the toppings ON a pizza.
		 * The list can then be added to the Pizza object elsewhere in the
		 */

		return null;	
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
        conn.close();
        p.setToppings(getToppingsOnPizza(p));
        p.setDiscounts(getDiscounts(p));
        connect_to_db();
        
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