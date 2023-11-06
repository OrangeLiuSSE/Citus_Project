import time

import psycopg2
from datetime import datetime
import sys
import numpy as np

conn = psycopg2.connect(
    host="xgph10",
    database="project",
    user="cs4224f",
    port=5102)

cursor = conn.cursor()


# list or tuple
def new_order_transaction(user_i, item_i, item_num):
    # Judge Local or Remote Order
    all_local = 1
    for item in item_i:
        if item[1] != user_i[1]:
            all_local = 0
            break

    # Get Current Time
    timestamp = datetime.now()

    # Get Customer and Tax Information
    customer_info_sql = "SELECT C_LAST, C_CREDIT, C_DISCOUNT " \
                        "FROM customer " \
                        "WHERE customer.W_ID = {} and customer.D_ID = {} and customer.C_ID = {} ".format(user_i[1], user_i[2], user_i[0])

    cursor.execute(customer_info_sql)
    result = cursor.fetchone()
    if result is not None:
        C_LAST, C_CREDIT, C_DISCOUNT = \
            result[0], result[1], result[2]
    else:
        pass

    warehouse_info_sql = "SELECT W_TAX FROM warehouse WHERE W_ID = {}".format(user_i[1])
    cursor.execute(warehouse_info_sql)
    result = cursor.fetchone()
    if result is not None:
        W_TAX = result[0]
    else:
        pass

    district_info_sql = "SELECT D_TAX, D_NEXT_O_ID FROM district WHERE W_ID = {} and D_ID = {}".format(user_i[1], user_i[2])
    cursor.execute(district_info_sql)
    result = cursor.fetchone()
    if result is not None:
        D_TAX, D_NEXT_O_ID = result[0], result[1]
    else:
        pass

    print(
        "Customer Identifier(W_ID, D_ID, C_ID): {}, Lastname: {}, Credit: {}, Discount: {}".format(user_i, C_LAST,
                                                                                                   C_CREDIT,
                                                                                                   C_DISCOUNT),
        end='\n')
    print("Warehouse Tax Rate: {}, District Tax Rate: {}".format(W_TAX, D_TAX), end='\n')

    # Update the D_NEXT_O_ID by 1 & Create a new order
    update_order_id_sql = "UPDATE district SET D_NEXT_O_ID = {} WHERE W_ID = {} and D_ID = {}".format(D_NEXT_O_ID + 1,
                                                                                                      user_i[1],
                                                                                                      user_i[2])

    cursor.execute(update_order_id_sql)

    # For every Item in the order, update stock and create new order line
    total_amount = 0
    item_index = 1

    create_new_order_sql = "INSERT INTO public.order VALUES({}, {}, {}, {}, null, {}, {}, '{}', null)".format(user_i[1],
                                                                                                              user_i[2],
                                                                                                              D_NEXT_O_ID,
                                                                                                              user_i[0],
                                                                                                              item_num,
                                                                                                              all_local,
                                                                                                              timestamp)
    cursor.execute(create_new_order_sql)

    item_id_info = []

    for item in item_i:
        item_id = item[0]
        item_id_info.append(item_id)
        warehouse_id = item[1]
        o_quantity = item[2]

        # Get information about the item
        get_amount_sql = "SELECT I_NAME, I_PRICE, S_QUANTITY " \
                         "FROM item, stock " \
                         "WHERE item.I_ID = {} and stock.W_ID = {} and item.I_ID = stock.I_ID".format(item_id,
                                                                                                      warehouse_id)
        cursor.execute(get_amount_sql)
        result = cursor.fetchone()
        item_name, price, s_quantity = result[0], result[1], result[2]
        item_amount = float(price) * float(o_quantity)
        total_amount = total_amount + item_amount

        # Update information of the item in the stock
        remote = 1 if warehouse_id != user_i[1] else 0
        adjust_quantity = int(s_quantity) - int(o_quantity)

        if adjust_quantity < 10:
            adjust_quantity = adjust_quantity + 100

        update_s_quantity = "UPDATE stock SET " \
                            "S_QUANTITY = {}, " \
                            "S_YTD = S_YTD + {}, " \
                            "S_ORDER_CNT = S_ORDER_CNT + 1, " \
                            "S_REMOTE_CNT = S_REMOTE_CNT + {} " \
                            "WHERE W_ID = {} and I_ID = {}".format(adjust_quantity, o_quantity,
                                                                   remote, warehouse_id, item_id)

        # Create new order line
        create_new_order_line_sql = "INSERT INTO order_line values({}, {}, {}, {}, {}, null, {}, {}, {}, '{}')".format(
            user_i[1], user_i[2], D_NEXT_O_ID, item_index, item_id,
            item_amount, warehouse_id, o_quantity, 'S_DIST_' + str(user_i[2]))

        cursor.execute(update_s_quantity)
        cursor.execute(create_new_order_line_sql)

        print(
            "Item No.{}, Item Number: {}, Item Name: {}, S_WAREHOUSE: {}, Quantity: {}, OL_AMOUNT:{}, S_QUANTITY: {}".format(
                item_index, item_id, item_name, warehouse_id, o_quantity, item_amount, adjust_quantity
            ), end='\n')
        item_index = item_index + 1

    total_amount = total_amount * (1 + float(D_TAX) + float(W_TAX)) * (1 - float(C_DISCOUNT))

    update_new_order_sql = "UPDATE public.order SET O_AMOUNT = {} WHERE W_ID = {} and D_ID = {} and O_ID = {}".format(
        total_amount, user_i[1], user_i[2], D_NEXT_O_ID)
    cursor.execute(update_new_order_sql)

    get_customer_order = "SELECT O_INFO FROM CUSTOMER_ORDER WHERE W_ID = {} and D_ID = {} and C_ID = {}".format(
        user_i[1], user_i[2], user_i[0])

    cursor.execute(get_customer_order)
    result = cursor.fetchone()[0]

    result = result + '!' + str(D_NEXT_O_ID) + ':' + ','.join(item_id_info)
    cursor.execute("UPDATE CUSTOMER_ORDER SET O_INFO = '{}' WHERE W_ID = {} and D_ID = {} and C_ID = {}".format(result, user_i[1], user_i[2], user_i[0]))

    print("Order Number: {}, Entry Data: {}, Number of Items: {}, Total Amount: {}".format(D_NEXT_O_ID, timestamp,
                                                                                           item_num, total_amount),
          end='\n')
    conn.commit()


# P,1,4,1135,2760.04
def payment_transaction(W_ID, D_ID, C_ID, PAYMENT):
    update_warehouse = "UPDATE warehouse SET W_YTD = W_YTD + {} WHERE W_ID = {}".format(PAYMENT, W_ID)
    update_district = "UPDATE district SET D_YTD = D_YTD + {} WHERE W_ID = {} and D_ID = {}".format(PAYMENT, W_ID, D_ID)
    update_customer = ("UPDATE customer SET C_YTD_PAYMENT = C_YTD_PAYMENT + {}, C_BALANCE = C_BALANCE - {}, "
                       "C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE W_ID = {} and D_ID = {} and C_ID = {}").format(PAYMENT,
                                                                                                               PAYMENT,
                                                                                                               W_ID,
                                                                                                               D_ID,
                                                                                                               C_ID)
    cursor.execute(update_warehouse)
    cursor.execute(update_district)
    cursor.execute(update_customer)

    information_sql = "SELECT C_FIRST, C_MIDDLE, C_LAST, " \
                      "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, " \
                      "C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, " \
                      "W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, " \
                      "D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP " \
                      "FROM customer c, district d, warehouse w " \
                      "WHERE c.W_ID = {} and c.D_ID = {} and c.C_ID = {} and c.W_ID = w.W_ID " \
                      "and c.D_ID = d.d_ID and c.W_ID = d.W_ID".format(W_ID, D_ID, C_ID)

    cursor.execute(information_sql)
    result = cursor.fetchone()

    customer_i, name, c_address, c_info, w_address, d_address = \
        (W_ID, D_ID, C_ID), result[:3], result[3:8], result[8:14], result[14:19], result[19:]

    print("Customer Identifier: {}, Name: {}, Address: {}, "
          "Information: {}, W_address: {}, D_address: {}, Payment: {}".format(
        customer_i, name, c_address, c_info, w_address, d_address, PAYMENT), end='\n')

    conn.commit()

# D,1,10
def delivery_transaction(W_ID, CARRIER_ID):
    timestamp = datetime.now()
    
    get_o_id = "SELECT D_ID, MIN(O_ID) FROM public.order WHERE W_ID = {} and O_CARRIER_ID IS NULL GROUP BY D_ID".format(W_ID)
    
    cursor.execute(get_o_id)
    result = cursor.fetchall()

    if len(result) != 0:
        list_result = str(result)
        list_result = list_result.replace('[', '(')
        list_result = list_result.replace(']', ')')

        update_order = "UPDATE public.order SET O_CARRIER_ID = {} WHERE W_ID = {} and (D_ID, O_ID) in {}".format(CARRIER_ID, W_ID, list_result)

        update_orderline = "UPDATE order_line SET OL_DELIVERY_D = '{}' WHERE W_ID = {} and (D_ID, O_ID) in {}".format(
            timestamp, W_ID, list_result)

        get_order_num = "SELECT D_ID, MIN(O_ID) FROM public.order WHERE W_ID = {} and O_CARRIER_ID IS NULL GROUP BY D_ID".format(
            W_ID)
        cursor.execute(get_order_num)
        results = cursor.fetchall()
        for result in results:
            cursor.execute(
                "UPDATE customer SET C_BALANCE = C_BALANCE + (SELECT O_AMOUNT FROM public.order WHERE W_ID = {} and D_ID = {} and O_ID = {}), "
                "C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE W_ID = {} and D_ID = {} and C_ID = "
                "(SELECT C_ID FROM public.order WHERE W_ID = {} and D_ID = {} and O_ID = {})".format(W_ID, result[0],
                                                                                                    result[1], W_ID,
                                                                                                    result[0], W_ID,
                                                                                                    result[0], result[1]))

        cursor.execute(update_order)
        cursor.execute(update_orderline)
        conn.commit()
    else:
        conn.commit()


def order_status_transaction(W_ID, D_ID, C_ID):
    basic_info_sql = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, O_ID, O_ENTRY_D, O_CARRIER_ID " \
                     "FROM customer c , public.order o " \
                     "WHERE c.W_ID = {} and c.D_ID = {} and c.C_ID = {} " \
                     "and o.W_ID = c.W_ID and o.D_ID = c.D_ID and o.C_ID = c.C_ID " \
                     "and o.O_ID = " \
                     "(SELECT MAX(O_ID) " \
                     "FROM public.order " \
                     "WHERE W_ID = c.W_ID and D_ID = c.D_ID and C_ID = c.C_ID)".format(W_ID, D_ID, C_ID)

    cursor.execute(basic_info_sql)
    basic_info = cursor.fetchone()

    item_info_sql = "SELECT I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D " \
                    "FROM order_line " \
                    "WHERE W_ID = {} and D_ID = {} and O_ID = {}".format(W_ID, D_ID, basic_info[4])
    cursor.execute(item_info_sql)
    items_info = cursor.fetchall()

    print("Customer: {}, Balance: {}, Last Order: {}".format(basic_info[:3], basic_info[3], basic_info[4:]), end='\n')

    for item in items_info:
        print("Item ID: {}, WarehouseID: {}, Quantity: {}, Amount: {}, Delivery Date: {}".format(
            item[0], item[1], item[2], item[3], item[4]
        ), end='\n')
    
    conn.commit()


# 5
def stock_level_transaction(w_id, d_id, l, t):
    try:
        cursor = conn.cursor()
        # Get the next available order number

        get_stock_num_sql = ("SELECT COUNT(I_ID) FROM Stock s WHERE s.W_ID = {} and s.I_ID IN "
                             "(SELECT I_ID FROM ORDER_LINE ol WHERE ol.W_ID = {} and ol.D_ID = {} and ol.O_ID in "
                             "(SELECT O_ID FROM public.order o WHERE o.W_ID = {} and o.D_ID = {} ORDER BY O_ID LIMIT {})) "
                             "and s.S_QUANTITY < {}").format(w_id, w_id, d_id, w_id, d_id, l, t)

        cursor.execute(get_stock_num_sql)
        result = cursor.fetchall()
        print(result)

        # cursor.execute("SELECT D_NEXT_O_ID FROM District WHERE district.W_ID = %s AND district.D_ID = %s", (w_id, d_id))
        # n = cursor.fetchone()[0]
        # # Get the set of items from the last L orders
        # cursor.execute("""
        #     SELECT I_ID
        #     FROM Order_Line
        #     WHERE order_line.D_ID = %s AND order_line.W_ID = %s AND order_line.O_ID BETWEEN %s AND %s
        # """, (d_id, w_id, n - l, n - 1))
        # items = cursor.fetchall()
        # items_tuple = tuple([item[0] for item in items])
        # print(items_tuple)
        # # Count items with stock quantity below threshold
        # cursor.execute("""
        #     SELECT COUNT(*)
        #     FROM Stock
        #     WHERE stock.I_ID IN %s AND stock.W_ID = %s AND s_QUANTITY < %s
        # """, (items_tuple, w_id, t))
        # count = cursor.fetchone()[0]
        # print(f"The the total number of items are {count}")
        # return count
        conn.commit()

    except Exception as error:
        print(f"Error: {error}")
        return None


# 6
def popular_item_transaction(W_ID, D_ID, L):
    try:
        cursor = conn.cursor()
        # Get the next available order number
        cursor.execute("SELECT D_NEXT_O_ID FROM District WHERE district.W_ID = %s AND district.D_ID = %s", (W_ID, D_ID))
        N = cursor.fetchone()[0]

        # Define a dictionary to store the popular item count
        popular_items_count = {}

        for x in range(N - L, N):
            cursor.execute("""
                SELECT I_ID, OL_QUANTITY
                FROM Order_Line
                WHERE order_line.W_ID = %s AND order_line.D_ID = %s AND order_line.O_ID = %s
                ORDER BY order_line.OL_QUANTITY DESC
                LIMIT 1
            """, (W_ID, D_ID, x))
            popular_item = cursor.fetchone()

            if popular_item[0] in popular_items_count:
                popular_items_count[popular_item[0]] += 1
            else:
                popular_items_count[popular_item[0]] = 1

        # Calculate the percentage of examined orders
        for item, count in popular_items_count.items():
            percentage = (count / L) * 100
            cursor.execute("SELECT I_NAME FROM Item WHERE I_ID = %s", (item,))
            item_name = cursor.fetchone()[0]
            print(f"Item Name: {item_name}, Percentage: {percentage}%")
        conn.commit()

    except Exception as e:
        print(f"Error in popular_item_transaction: {e}")


# 7
def top_balance_transaction():
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, W_NAME, D_NAME 
            FROM Customer, Warehouse, District 
            WHERE Customer.W_ID = Warehouse.W_ID AND Customer.D_ID = District.D_ID
            ORDER BY Customer.C_BALANCE DESC
            LIMIT 10
        """)

        top_customers = cursor.fetchall()
        for customer in top_customers:
            print(
                f"Customer Name: {customer[0]} {customer[1]} {customer[2]}, Balance: {customer[3]}, Warehouse: {customer[4]}, District: {customer[5]}")
        conn.commit()

    except Exception as e:
        print(f"Error in top_balance_transaction: {e}")


# 8
def related_customer_transaction(C_W_ID, C_D_ID, C_ID):
    try:
        cursor = conn.cursor()
        related_customers = set()

        # Fetch all order IDs for the given customer
        cursor.execute("""SELECT O_ID FROM "order" WHERE "order".W_ID = %s AND "order".D_ID = %s AND "order".C_ID = %s""",
                       (C_W_ID, C_D_ID, C_ID))
        orders = cursor.fetchall()

        for order in orders:
            cursor.execute("SELECT I_ID FROM Order_Line WHERE order_line.W_ID = %s AND order_line.D_ID = %s AND order_line.O_ID = %s",
                           (C_W_ID, C_D_ID, order[0]))
            items = cursor.fetchall()

            for item in items:
                cursor.execute("""
                    SELECT DISTINCT C_ID 
                    FROM "order", Order_Line 
                    WHERE "order".O_ID = Order_Line.O_ID AND "order".W_ID = Order_Line.W_ID AND "order".D_ID = Order_Line.D_ID
                    AND Order_Line.I_ID = %s AND "order".W_ID != %s
                """, (item[0], C_W_ID))
                related_customers.update(cursor.fetchall())

        for customer in related_customers:
            print(f"Related Customer ID: {customer[0]}")
        conn.commit()

    except Exception as e:
        print(f"Error in related_customer_transaction: {e}")


# R,1,3,314
def related_customer_transaction_1(W_ID, D_ID, C_ID):
    original_customer_order_sql = "SELECT O_INFO FROM CUSTOMER_ORDER WHERE W_ID = {} and D_ID = {} and C_ID = {}".format(
        W_ID, D_ID, C_ID)
    cursor.execute(original_customer_order_sql)
    o_cus_order = cursor.fetchone()[0]
    o_cus_order_info = o_cus_order.split('!')

    other_cus_sql = "SELECT C_ID, O_INFO FROM CUSTOMER_ORDER WHERE W_ID != {}".format(W_ID)
    cursor.execute(other_cus_sql)
    other_cus_results = cursor.fetchall()
    related_customer = []

    for o_c_order in o_cus_order_info:
        order_item = o_c_order.split(':')[1].split(',')
        for other_cus_result in other_cus_results:
            C_ID = other_cus_result[0]
            other_order_info = other_cus_result[1].split('!')
            for other_order in other_order_info:
                other_order_item = other_order.split(':')[1].split(',')
                if len(set(order_item) & set(other_order_item)) >= 2:
                    if C_ID not in related_customer:
                        related_customer.append(C_ID)
                    break
    print(related_customer)
    conn.commit()


if len(sys.argv) > 1:
    file_path = sys.argv[1]
    with open(file_path, 'r') as file:
        transactions = file.readlines()
        file.close()
    start_time = datetime.now()
    total_num = 0
    trans_latency = []
    for (index, xact) in enumerate(transactions):
        xact = xact.replace('\n', '').replace('\r', '').split(',')
        xact_ident = xact[0]
        xact_params = xact[1:]
        local_start_time = datetime.now()
        if xact_ident == 'P':
            total_num = total_num + 1
            payment_transaction(xact_params[0], xact_params[1], xact_params[2], xact_params[3])
            trans_latency.append((datetime.now()-local_start_time).total_seconds() * 1000.0)
        elif xact_ident == 'D':
            total_num = total_num + 1
            delivery_transaction(xact_params[0], xact_params[1])
            trans_latency.append((datetime.now()-local_start_time).total_seconds() * 1000.0)
        elif xact_ident == 'O':
            total_num = total_num + 1
            order_status_transaction(xact_params[0], xact_params[1], xact_params[2])
            trans_latency.append((datetime.now()-local_start_time).total_seconds() * 1000.0)
        elif xact_ident == 'N':
            total_num = total_num + 1
            total_item_num = xact_params[-1]
            user_info = xact_params[:-1]
            item_info = []
            for n_index in range(index + 1, index + 1 + int(total_item_num)):
                item_info.append(transactions[n_index].replace('\n', '').replace('\r', '').split(','))
            new_order_transaction(user_info, item_info, total_item_num)
            trans_latency.append((datetime.now()-local_start_time).total_seconds() * 1000.0)
        elif xact_ident == 'S':
            total_num = total_num + 1
            stock_level_transaction(int(xact_params[0]), int(xact_params[1]), int(xact_params[2]), int(xact_params[3]))
            trans_latency.append((datetime.now()-local_start_time).total_seconds() * 1000.0)
        elif xact_ident == 'I':
            total_num = total_num + 1
            popular_item_transaction(int(xact_params[0]), int(xact_params[1]), int(xact_params[2]))
            trans_latency.append((datetime.now()-local_start_time).total_seconds() * 1000.0)
        elif xact_ident == 'T':
            total_num = total_num + 1
            top_balance_transaction()
            trans_latency.append((datetime.now()-local_start_time).total_seconds() * 1000.0)
        elif xact_ident == 'R':
            total_num = total_num + 1
            related_customer_transaction_1(xact_params[0], xact_params[1], xact_params[2])
            trans_latency.append((datetime.now()-local_start_time).total_seconds() * 1000.0)
        else:
            pass
        
    total_time = (datetime.now() - start_time).total_seconds()
    print("\nStatistic Summary:")
    print('Total Transaction Time: ' + str(total_time) + "(/s)")
    print('Total Transaction Number: ' + str(total_num))
    print(f"Transaction throughput: {total_num / total_time} (/s)")
    print(f"Average transaction latency: {np.mean(trans_latency)} (ms)")
    print(f"Median transaction latency: {np.median(trans_latency)} (ms))")
    print(f"95th percentile transaction latency: {np.percentile(trans_latency, 95)} (ms)")
    print(f"99th percentile transaction latency: {np.percentile(trans_latency, 99)} (ms)")
    
else:
    print('No File Path....')
