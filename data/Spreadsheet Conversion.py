#This is a file for the Walmart Global Tech program
import csv
import sqlite3

#Converts the Shipping_data_0 file to a database file
def convert_file():

    with open('shipping_data_0.csv', newline='') as csvfile:
        datareader = csv.reader(csvfile, delimiter=' ')
    
        con = sqlite3.connect('ship_data_0.db')
        cur = con.cursor()
        
        cur.execute("CREATE TABLE shipping_data_0 (orgin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier)")

        for row in datareader:
            x = str(','.join(row))
            y = x.split(',')

            entry = [
                (y[0]), 
                (y[1]), 
                (y[2]), 
                (y[3]), 
                (y[4]), 
                (y[5])
                ]
       
            #print(entry)
            cur.execute("insert into shipping_data_0 values (?,?,?,?,?,?)", entry)

        con.commit()
        con.close()

#Spreadsheet 1 combine each row based on shipping identifier, quantity, and add new row
def combine_data():
        with open('temp.csv','w', newline='') as csvfile:
            tempreader = csv.writer(csvfile, delimiter=' ')

            #Checks to see if the data repeats, and add a quantity
            with open('shipping_data_1.csv',newline='') as csvfile:
                dreader1 = csv.reader(csvfile, delimiter=' ')

                count = 1
                last_element = "there"
                f_iter = True

                for row in dreader1:
                    x = str(','.join(row))
                    y = x.split(',')

                    if (f_iter == True):
                        entry = [
                        (y[0]), 
                        (y[1]), 
                        (y[2]), 
                        ]

                        tempreader.writerow([entry[0], entry[1], entry[2], "quantity"])
                        f_iter = False
                    else:
                        if (last_element == y[1]):
                            count = count + 1
                        else:
                            tempreader.writerow([entry[0], entry[1], entry[2], count])
                            count = 1
        
                    entry = [
                        (y[0]), 
                        (y[1]), 
                        (y[2]), 
                        ]

                    last_element = y[1]
            
                    #print(entry)


#Combines the orgin warehouse and the destination store from the shipping data 2
def combine_files():
    with open('temp2.csv', 'w',newline='') as csvfile:
        f_write = csv.writer(csvfile, delimiter=' ')    
        with open('shipping_data_2.csv', newline='') as csvfile:
            ship2 = csv.reader(csvfile, delimiter=' ')
            with open('temp.csv', newline='') as csvfile:
                temp = csv.reader(csvfile, delimiter=' ')

                for t_row in temp:
                    temp_row = str(','.join(t_row))
                    temp_row = temp_row.split(',')
                    temp_dic = [
                        (temp_row[0]),
                        (temp_row[1]),
                        (temp_row[2]),
                        (temp_row[3])
                    ]

                    for s_data in ship2:
                        data = str(','.join(s_data))
                        data = data.split(',')
                        data_dic = [
                            (data[0]),
                            (data[1]),
                            (data[2]),
                            (data[3])
                        ]
                        if (temp_dic[0] == data_dic[0]):
                            f_write.writerow([data_dic[1], data_dic[2], temp_dic[0], temp_dic[1], temp_dic[2], data_dic[3], data_dic[0]])
                            print(data_dic[1], data_dic[2], temp_dic[0], temp_dic[1], temp_dic[2], data_dic[3], data_dic[0])

#Convert the temp file into a database file
def convert_temp_file():

    with open('temp.csv', newline='') as csvfile:
        datareader = csv.reader(csvfile, delimiter=' ')
    
        con = sqlite3.connect('ship_data_1.db')
        cur = con.cursor()
        
        cur.execute("CREATE TABLE shipping_data_1 (ship_identifier, product, on_time, product_quantity)")

        for row in datareader:
            x = str(','.join(row))
            y = x.split(',')

            entry = [
                (y[0]), 
                (y[1]), 
                (y[2]), 
                (y[3])
                ]
       
            cur.execute("insert into shipping_data_1 values (?,?,?,?)", entry)

        con.commit()
        con.close()

#execute
def main():
    convert_file()
    combine_data()
    convert_temp_file()

main()