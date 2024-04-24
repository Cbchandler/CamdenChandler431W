import psycopg2

connection = psycopg2.connect(database="SteamGames", user="postgres", password="7964", host="localhost", port=5432)

cursor = connection.cursor()

print("Welcome to the Database CLI!\n")
print("\nPlease select an option:")

import numpy as np

def inputNumber(prompt):

    while True:
        
        try:
            num = float(input(prompt))
            break
        except ValueError:
            pass
    
    return  num
        
def displayMenu(options):
    for i in range(len(options)):
        print("{:d}. {:s}".format(i+1, options[i]))
    
    choice = 0
    while not (np.any(choice == np.arange(len(options))+1)):
        choice = inputNumber ("Enter Your Choice (1-12): ")
    
    return choice

def DeletedisplayMenu(options):
    for i in range(len(options)):
        print("{:d}. {:s}".format(i+1, options[i]))
    
    choice = 0
    while not (np.any(choice == np.arange(len(options))+1)):
        choice = inputNumber ("Enter Your Choice (1-2): ")
    
    return choice


def AggregateDisplayMenu(options):
    for i in range(len(options)):
        print("{:d}. {:s}".format(i+1, options[i]))
    
    choice = 0
    while not (np.any(choice == np.arange(len(options))+1)):
        choice = inputNumber ("Enter Your Choice (1-6): ")
    
    return choice

def JoinDisplayMenu(options):
    for i in range(len(options)):
        print("{:d}. {:s}".format(i+1, options[i]))
    
    choice = 0
    while not (np.any(choice == np.arange(len(options))+1)):
        choice = inputNumber ("Enter Your Choice (1-6): ")
    
    return choice

def TransactionDisplayMenu(options):
    for i in range(len(options)):
        print("{:d}. {:s}".format(i+1, options[i]))
    
    choice = 0
    while not (np.any(choice == np.arange(len(options))+1)):
        choice = inputNumber ("Enter Your Choice (1-7): ")
    
    return choice

menuItems = np.array(["Instert Data", "Delete Data", "Update Data", "Search Data", "Aggregate Functions", "Sorting", "Joins", "Grouping", "Subqueries", "Transations", "Error Handling", "Exit"])
deleteMenu = np.array(["Delete", "Drop", "Quit"])
AggregateMenu = np.array(["Sum", "Average", "Count", "Min", "Max", "Quit"])
JoinMenu = np.array(["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "Quit"])
TransactionMenu = np.array(["Begin Transaction", "Commit Transaction", "Rollback Transaction", "Savepoint", "Release Savepoint", "Set Transaction", "Exit"])

while True:

    choice = displayMenu(menuItems);
    
#User selects options "Insert" Function
    if choice == 1:
        game_name = input("Enter Game Title: ")
        game_id = input("Enter AppID: ")
        game_price = input("Enter Game Price: ")
        game_achievement_count = input("Enter The Number Of Achievements: " )
        game_category = input("Enter Game Categories: ")
        game_genre = input("Enter Game Genres: ")
        game_release_date = input("Enter Game Release Date: ")
        
        sql = "INSERT INTO games (game_name, game_price, game_release_date, appid, achievement_count, categories, genre) VALUES (%s, %s, %s, %s, %s, %s, %s) "
        vals = (game_name, game_price, game_release_date, game_id, game_achievement_count, game_category, game_genre)
        
        cursor.execute(sql, vals)

        #connection.commit()
        
        cursor.execute("Select * FROM games")
        rows = cursor.fetchall()
        
        for r in rows:
            print (f"Game Name {r[0]}, Price {r[1]}, Release Date {r[2]}, appid {r[3]}, Achievement_Count {r[4]}, Categories {r[5]}, Genre {r[6]}")
        

        

#User Selects option 2 "Delete" Function
    elif choice == 2:
        choice = DeletedisplayMenu(deleteMenu)
        if choice == 1:
            game_id = input("\nType Quit To Leave\n\nEnter Appid: ")
            if game_id == "Quit":
                break
                

            cursor.execute("DELETE from games WHERE appid = (%s)", (game_id,))
            cursor.execute("Select * FROM games")
            rows = cursor.fetchall()
        
            for r in rows:
                print (f"Game Name {r[0]}, Price {r[1]}, Release Date {r[2]}, appid {r[3]}, Achievement_Count {r[4]}, Categories {r[5]}, Genre {r[6]}")
        

        
       

        #connection.commit()

        elif choice == 2:
            column_name = input("\nType Quit To Leave\n\nEnter Column Name: ")
            if column_name == "Quit":
                break
                
            sqlDrop = f"ALTER TABLE games DROP COLUMN {column_name}"
            cursor.execute(sqlDrop)


            cursor.execute("Select * FROM games")
            
            
            rows = cursor.fetchall()
        
            for r in rows:
                print (f"Game Name {r[0]}, Price {r[1]}, Release Date {r[2]}, appid {r[3]}, Achievement_Count {r[4]}, Categories {r[5]}")
        
    elif choice == 3:
        price = input("Sale for Game: ")
        game_id = input("Game Id: ")
        sql_update = """Update games set game_price = %s where appid = %s"""
        cursor.execute(sql_update, (price, game_id))
        cursor.execute("Select * FROM games")
        rows = cursor.fetchall()
        
        for r in rows:
           print (f"Game Name {r[0]}, Price {r[1]}, Release Date {r[2]}, appid {r[3]}, Achievement_Count {r[4]}, Categories {r[5]}, Genre {r[6]}")
        

        #connection.commit()
    
    elif choice == 4:
        Column = input("Enter the column name to search in: ")
        Criteria = input(f"Enter the search criteria for {Column}: ")

        valid_columns = ['game_name', 'game_price', 'game_release_date', 'appid', 'achievement_count', 'categories', 'genre']
        if Column not in valid_columns:
            print("Invalid column name!")
        else:
            sqlSearch = f"SELECT * FROM games WHERE {Column} = %s"
            cursor.execute(sqlSearch, (Criteria,))
            rows = cursor.fetchall()
            for r in rows:
                print(f"Game Name: {r[0]}, Price: {r[1]}, Release Date: {r[2]}, appid: {r[3]}, Achievement Count: {r[4]}, Categories: {r[5]}, Genre: {r[6]}")
        
        #connection.commit()
   
    elif choice == 5:
        while True:
            choice = AggregateDisplayMenu(AggregateMenu);
        
            if choice == 1:
                Column = input("What Would You Like The Sum Of?: ")
                valid_columns = ['game_price', 'achievement_count', 'appid'] 
                if Column not in valid_columns:
                    print("Invalid column name!")
                else:
                    sqlSum = f"SELECT SUM({Column}) FROM games"
                    cursor.execute(sqlSum)
                    sum_result = cursor.fetchone()[0]
                    print("\nSum: ", sum_result)
        
            elif choice == 2:
                Column = input("What Would You Like The Average Of?: ")
                valid_columns = ['game_price', 'achievement_count', 'appid'] 
                if Column not in valid_columns:
                    print("Invalid column name!")
                else:
                    sqlAVG = f"SELECT AVG({Column}) FROM games"
                    cursor.execute(sqlAVG)
                    AVG_result = cursor.fetchone()[0]
                    print("\nAVG: ", AVG_result) 
        
            elif choice == 3:
                Column = input("What Would You Like to Count?: ")
                valid_columns = ['game_price', 'achievement_count', 'appid', "game_name", "Genre"]  
                if Column not in valid_columns:
                    print("Invalid column name!")
                else:
                    sqlCount = f"SELECT COUNT({Column}) FROM games"
                    cursor.execute(sqlCount)
                    Count_result = cursor.fetchone()[0]
                    print("\nCount: ", Count_result)  
        
            elif choice == 4:
                Column = input("What Would You Like The Min Value of?: ")
                valid_columns = ['game_price', 'achievement_count', 'appid']  
                if Column not in valid_columns:
                    print("Invalid column name!")
                else:
                    sqlMin = f"SELECT MIN({Column}) FROM games"
                    cursor.execute(sqlMin)
                    Min_result = cursor.fetchone()[0]
                    print("\nMin: ", Min_result)     
        
            elif choice == 5:
                Column = input("What Would You Like The Max Value of?: ")
                valid_columns = ['game_price', 'achievement_count', 'appid']
                if Column not in valid_columns:
                    print("Invalid column name!")
                else:
                    sqlMax = f"SELECT Max({Column}) FROM games"
                    cursor.execute(sqlMax)
                    Max_result = cursor.fetchone()[0]
                    print("\nMax: ", Max_result)          
        
            elif choice == 6:
                break
    


    elif choice == 6:
        sort_column = input("Enter the column name to sort by: ")
        sort_order = input("Enter Order ASC or DESC: ")

        if sort_order not in ['ASC', 'DESC']:
            print("Invalid")
        else:
            sqlSort = f"SELECT * FROM games ORDER BY {sort_column} {sort_order}"
            cursor.execute(sqlSort)
            sorted_rows = cursor.fetchall()
            for r in sorted_rows:
                print(f"Game Name: {r[0]}, Price: {r[1]}, Release Date: {r[2]}, appid: {r[3]}, Achievement Count: {r[4]}, Categories: {r[5]}, Genre: {r[6]}")
    elif choice == 7:
        choice = JoinDisplayMenu(JoinMenu);
        if choice == 1:
            table1 = input("Enter the first table name: ")
            table2 = input("Enter the second table name: ")
            tableJoin = input("Enter Join Conditioning: ")
            sqlJoin = f"SELECT * FROM {table1} INNER JOIN {table2} ON {tableJoin}"
            cursor.execute(sqlJoin)
            sorted_rows = cursor.fetchall()
            for r in sorted_rows:
                print(f"Game Name: {r[0]}, Price: {r[1]}, Release Date: {r[2]}, appid: {r[3]}, Achievement Count: {r[4]}, Categories: {r[5]}, Genre: {r[6]}, DLC Count {r[7]}, Mac {r[8]}, Windows {r[9]} , Linux {r[10]}")    
        elif choice == 2:
            table1 = input("Enter the first table name: ")
            table2 = input("Enter the second table name: ")
            tableJoin = input("Enter Join Conditioning: ")
            sqlJoin = f"SELECT * FROM {table1} LEFT JOIN {table2} ON {tableJoin}"
            cursor.execute(sqlJoin)
            sorted_rows = cursor.fetchall()
            for r in sorted_rows:
                print(f"Game Name: {r[0]}, Price: {r[1]}, Release Date: {r[2]}, appid: {r[3]}, Achievement Count: {r[4]}, Categories: {r[5]}, Genre: {r[6]}, DLC Count {r[7]}, Mac {r[8]}, Windows {r[9]} , Linux {r[10]}")            
    
        elif choice == 3:
            table1 = input("Enter the first table name: ")
            table2 = input("Enter the second table name: ")
            tableJoin = input("Enter Join Conditioning: ")
            sqlJoin = f"SELECT * FROM {table1} RIGHT JOIN {table2} ON {tableJoin}"
            cursor.execute(sqlJoin)
            sorted_rows = cursor.fetchall()
            for r in sorted_rows:
                print(f"Game Name: {r[0]}, Price: {r[1]}, Release Date: {r[2]}, appid: {r[3]}, Achievement Count: {r[4]}, Categories: {r[5]}, Genre: {r[6]}, DLC Count {r[7]}, Mac {r[8]}, Windows {r[9]} , Linux {r[10]}")            
   

        elif choice == 4:
            table1 = input("Enter the first table name: ")
            table2 = input("Enter the second table name: ")
            tableJoin = input("Enter Join Conditioning: ")
            sqlJoin = f"SELECT * FROM {table1} FULL JOIN {table2} ON {tableJoin}"
            cursor.execute(sqlJoin)
            sorted_rows = cursor.fetchall()
            for r in sorted_rows:
                print(f"Game Name: {r[0]}, Price: {r[1]}, Release Date: {r[2]}, appid: {r[3]}, Achievement Count: {r[4]}, Categories: {r[5]}, Genre: {r[6]}, DLC Count {r[7]}, Mac {r[8]}, Windows {r[9]} , Linux {r[10]}")            
   
        elif choice == 5:
            break
    elif choice == 8:
        Column1 = input("Enter First Column: ")
        Column2 = input("Enter Second Column: ")
        tableSQL = input("Enter the table name: ")
        aggregationSelect = input("Select the aggregation functions (COUNT, SUM, AVG, MIN, MAX) :")
        sqlGroup = f"SELECT {Column1}, {aggregationSelect}({Column2}) FROM {tableSQL} GROUP BY {Column1}" 
        cursor.execute(sqlGroup)
        sorted_rows = cursor.fetchall()
        for r in sorted_rows:
            print(f"Game Name: {r[0]}, Price: {r[1]}")

    elif choice == 9:
        table1 = input("Enter Main Table: ")
        table2 = input("Enter Sub-Table: ")
        MainColumn = input("Enter column for Main: ")
        SubColumn = input("Enter column for Sub: ")
        sqlSub = f"(SELECT {SubColumn} FROM {table2})"
        sqlMain = f"SELECT * FROM {table1} WHERE {MainColumn} IN {sqlSub}"
        cursor.execute(sqlMain)
        sorted_rows = cursor.fetchall()  

        for r in sorted_rows:
                print(f"Game Name: {r[0]}, Price: {r[1]}, Release Date: {r[2]}, appid: {r[3]}, Achievement Count: {r[4]}, Categories: {r[5]}, Genre: {r[6]}")

    elif choice == 10:
        while True:
            choice = TransactionDisplayMenu(TransactionMenu)
        
            if choice == 1:
                cursor.execute("BEGIN TRANSACTION;")
                print("Transaction begun.")
    
            elif choice == 2:
                 cursor.execute("COMMIT;")
                 print("Transaction committed.")
        
            elif choice == 3:
                cursor.execute("ROLLBACK;")
                print("Transaction rolled back.")
        
            elif choice == 4:
                savepoint_name = input("Enter Savepoint Name: ")
                cursor.execute(f"SAVEPOINT {savepoint_name};")
                print(f"Savepoint '{savepoint_name}' created.")
        
            elif choice == 5:
                savepoint_name = input("Enter Savepoint Name to Release: ")
                cursor.execute(f"RELEASE SAVEPOINT {savepoint_name};")
                print(f"Savepoint '{savepoint_name}' released.")
        
            elif choice == 6:
                transaction_option = input("Enter Transaction Option: ")
                cursor.execute(f"SET TRANSACTION {transaction_option};")
                print(f"Transaction set to '{transaction_option}'.")
        
            elif choice == 7:
                break
    elif choice == 11:
        break
    elif choice == 12:
        break

