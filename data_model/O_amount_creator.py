import psycopg2


conn = psycopg2.connect(
    host="localhost",
    database="project",
    user="cs4224f",
    port=5102)

cursor = conn.cursor()

update_sql = ("UPDATE public.order SET O_AMOUNT = (SELECT SUM(OL_AMOUNT) "
              "FROM order_line WHERE public.order.W_ID = order_line.W_ID and public.order.D_ID = order_line.D_ID "
              "and public.order.O_ID = order_line.O_ID)")

cursor.execute(update_sql)

conn.commit()