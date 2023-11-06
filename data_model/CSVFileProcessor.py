# Author : Liu Zhicheng

import csv

with open(r"/home/stuproj/cs4224f/proj_data/project_files/data_files/order.csv", 'r') as csvfile:
    reader = csv.reader(csvfile)
    with open(r"/home/stuproj/cs4224f/proj_data/project_files/data_files/new_order.csv", 'w', newline='') as newfile:
        for row in reader:
            if row[4] == 'null':
                row[4] = ''
            writer = csv.writer(newfile)
            writer.writerow(row)

with open(r"/home/stuproj/cs4224f/proj_data/project_files/data_files/order-line.csv", 'r') as csvfile:
    reader = csv.reader(csvfile)
    with open(r"/home/stuproj/cs4224f/proj_data/project_files/data_files/new_order-line.csv", 'w', newline='') as newfile:
        for row in reader:
            if row[5] == 'null':
                row[5] = ''
            writer = csv.writer(newfile)
            writer.writerow(row)