# Rules-Engine-using-Scala

## Discount Rules Engine for Retail Store

The Discount Rules Engine for the retail store is a Scala application designed to automate the discount calculation process based on specific qualifying rules. This application reads order data from a CSV file, applies discount rules, calculates final prices, and inserts the processed data into an Oracle database.

#### Features:

- **Discount Rules**: Implements various discount rules based on product types, remaining days before expiry, quantity sold, and special dates.
- **Database Interaction**: Utilizes Oracle JDBC driver to connect to a database and insert processed order data.
- **Logging Mechanisms**: Logs engine rule interactions and errors to a text file for debugging and auditing purposes.

### Discount Rules Implemented:

1. **More Than 5 Qualifier Rule**:
   - Checks if the quantity of a product in an order is more than 5.
   - Discount calculation: 
     - 5% discount for quantities 6-9 units.
     - 7% discount for quantities 10-14 units.
     - 10% discount for quantities more than 15 units.

2. **Cheese and Wine Qualifier Rule**:
   - Identifies orders containing wine or cheese products.
   - Discount calculation: 
     - 5% discount for wine products.
     - 10% discount for cheese products.

3. **Less Than 30 Days to Expiry Qualifier Rule**:
   - Checks if there are less than 30 days remaining for the product to expire.
   - Discount calculation: Gradual discount based on days remaining, starting from 1% and increasing by 1% per day, up to a maximum of 30%.

4. **Products Sold on 23rd of March Qualifier Rule**:
   - Identifies orders made on the 23rd of March.
   - Discount calculation: 50% discount for orders made on this date.

5. **App Usage Qualifier Rule**:
   - Checks if the sale was made through the App.
   - Discount calculation: 
     - 5% discount for quantities 1-5 units.
     - 10% discount for quantities 6-10 units.
     - 15% discount for quantities more than 10 units.

6. **Visa Card Usage Qualifier Rule**:
   - Identifies orders made using Visa cards.
   - Discount calculation: 5% discount.

### Database Table Creation Statement:

```sql
CREATE TABLE orders (
    order_date DATE,
    expiry_date DATE,
    days_to_expiry NUMBER,
    product_category VARCHAR2(100),
    product_name VARCHAR2(100),
    quantity NUMBER,
    unit_price NUMBER,
    channel VARCHAR2(100),
    payment_method VARCHAR2(100),
    discount NUMBER,
    total_price NUMBER
);
```

This table structure is used to store the processed order data, including order details, discounts, and total prices. Adjust data types and sizes as needed based on your specific requirements.

#### How to Use:

1. **Clone Repository**:
   ```
   git clone https://github.com/HusseinK01/Rules-Engine-using-Scala.git
   ```

2. **Import Project**: Import the project into your IntelliJ IDE.

3. **Database Configuration**:
   - Update the database connection details in the code (`DiscountsEngine.scala`) with your Oracle database URL, username, and password.

4. **Run Application**:
   - Compile and run the `DiscountsEngine.scala` file to execute the discount calculation process.
   - Ensure that the required dependencies are installed and the CSV file containing order data (`TRX1000.csv`) is available in the specified location.

5. **Verify Results**:
   - Check the `orders` table in your Oracle database for inserted records with calculated discounts and total prices.
   - Review the `logs.log` file for logged engine rule interactions and any error messages.

---
