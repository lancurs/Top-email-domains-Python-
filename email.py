from __future__ import division
__author__ = 'pengyang'

#python version 2.7.10

import datetime
import mysql.connector
import os

# Assume the entry date of data of table "mailing" is known for test purpose
# In practice,As the New addresses will be added on a daily basis,the entry date could be recored simultaneously. 
def readfile(file):
    domain_list = []
    domain_daily_count = {}
    try:
        with open(file) as file:

            for each_line in file:
                try:
                    (addr, date) = each_line.strip().split(',', 1)
                    domain_list.append([date, addr[addr.find("@"):]])
                except ValueError:
                    pass

        for k, v in domain_list:
            domain_daily_count.setdefault(k, []).append(v)

    except IOError as err:
        print('IO error:' + str(err))

    return domain_daily_count


def insert(domain_daily_count):
    import mysql.connector

    try:
        cnx = mysql.connector.connect(user='root', password='1234',
                                      host='127.0.0.1', database='email')
    except mysql.connector.Error as err:

        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    cursor = cnx.cursor()
    x = {}

    for k, v in domain_daily_count.items():
        a = v
        for i in a:
            if a.count(i) > 1:
                x[i] = a.count(i)
        for key, value in x.iteritems():
            query = "INSERT INTO domain(domain,count,date) VALUES(%s,%s,%s);"
            data = (key, value, k)
            cursor.execute(query, data)
    cnx.commit()
    cnx.close()


# Calculate the percentage growth of the last 30 days compared to the total
# Use the table created by insert()
def report():
    now = datetime.datetime.now().date()
    thirty_days_before = now - datetime.timedelta(30)
    try:
        cnx = mysql.connector.connect(user='root', password='1234',
                                      host='127.0.0.1', database='email')
    except mysql.connector.Error as err:

        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    top_domain = {}  # the top domains by count:<key,value>=<domain,total_conut>
    top_domain_old = {}  # the top domains 30days before

    cursor = cnx.cursor()
    query = ("SELECT * FROM domain ORDER BY date DESC")
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        if row[1] not in top_domain:  # schema id|domain|count|date
            top_domain[row[1]] = row[2]

        else:
            top_domain[row[1]] += row[2]

        if row[3] < thirty_days_before:
            if row[1] not in top_domain_old:
                top_domain_old[row[1]] = row[2]
            else:
                top_domain_old[row[1]] += row[2]
    cursor.close()
    cnx.close()

    total = sum(top_domain.values())
    total_old = sum(top_domain_old.values())
    
    top_domain = sorted(top_domain.items(), key=lambda d: d[1], reverse = True)
    del top_domain[50:]

    res = []#result
    
    for each_domain in top_domain:#each_domain={<domain,count>}
        try:
            growth = round(((each_domain[1] / total - top_domain_old.get(each_domain[0]) / total_old)/(top_domain_old.get(each_domain[0]) / total_old) * 100),2)
            res.append([str(each_domain[0]),each_domain[1],growth])
        except StopIteration:
            growth = round((each_domain[1] / total-top),2) * 100
            res.append([str(each_domain[0]),each_domain[1],growth])
        
        
        
    res = sorted(res,key=lambda d:d[2],reverse=True)
 
    return res
    

def output(list):
    try:
        f = open('output.txt','w')
        k = ' '.join(['domain','count','growth'])
        f.write(k+"\n")
        for i in list:
            k = ' '.join([str(j) for j in i])
            f.write(k+"\n")
        f.close()
    except IOError:
        pass

def main():
    insert(readfile("input.txt"))
    print("Table updated sucessfully!\n")
    output(report())
    print("Report generated sucessfully!See:" + os.getcwd() + "/output.txt\n\n")


if __name__ == "__main__":
    main()
