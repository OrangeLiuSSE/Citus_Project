import psycopg2


conn = psycopg2.connect(
    host="localhost",
    database="project",
    user="cs4224f",
    port=5102)

#    order_id:1,2,3,4,5,6,7,8,9!order_id:1,2,3,4,5,6,7,8,9
if __name__ == '__main__':
    cursor = conn.cursor()

    cursor.execute("SELECT W_ID, D_ID, C_ID FROM customer")

    results = cursor.fetchall()

    for result in results:
        order_info = ""
        cursor.execute("SELECT O_ID FROM public.order WHERE W_ID = {} and D_ID = {} and C_ID = {}".format(result[0], result[1], result[2]))
        order_results = cursor.fetchall()
        for order_result in order_results:
            order_id = order_result[0]
            order_info = order_info + str(order_id) + ":"
            cursor.execute("SELECT I_ID FROM order_line WHERE W_ID = {} and D_ID = {} and O_ID = {}".format(result[0], result[1], order_id))
            item_results = cursor.fetchall()
            for item_result in item_results:
                item_id = item_result[0]
                order_info = order_info + str(item_id) + ","
            order_info = order_info[:-1] + '#'
        order_info = order_info[:-1]
        cursor.execute("INSERT INTO CUSTOMER_ORDER VALUES ({}, {}, {}, '{}')".format(result[0], result[1], result[2], order_info))

    conn.commit()