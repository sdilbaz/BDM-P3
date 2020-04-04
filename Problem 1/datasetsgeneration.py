import random
import string
import csv

def employeegeneration(): 
    with open('employee.csv', mode = 'w') as emp: 
        csvwrite = csv.writer(emp)
        for id in range(1,50001):
            stringrange = string.ascii_letters
            name = ( ''.join(random.choice(stringrange) for i in range(random.randint(10,15))))
            age = random.randint(18,100)
            countrycode = random.randint(1,500)
            salary = round(random.uniform(100.0, 10000000.0),2)
            employee = [id, name,age, countrycode, salary]
            csvwrite.writerow(employee)

def transactiongeneration():
    with open('transaction.csv', mode ='w') as trx:
        csvwrite = csv.writer(trx)
        for transid in range(1,5000001):
            custid = random.randint(1,50000)
            transtotal = round(random.uniform(10.0, 2000.0),2)
            transnumitem = random.randint(1,15)
            stringrange = string.ascii_letters
            transdesc = ''.join(random.choice(stringrange) for i in range(random.randint(1,15)))
            transaction = [transid, custid, transtotal, transnumitem, transdesc]
            csvwrite.writerow(transaction)
employeegeneration()
transactiongeneration()