

import httplib
import xml.etree.ElementTree as ET
import StringIO
import MySQLdb
from datetime import datetime
import pprint

# Configuration
config_file = 'config.xml'
token = "xxxxxxxxxxxxxxxx"
db_host = 'localhost'
db_user = 'platby'
db_pass = 'platby'
db_name = 'platby'

class MyPrettyPrinter(pprint.PrettyPrinter):
    def format(self, object, context, maxlevels, level):
        if isinstance(object, unicode):
            return (object.encode('utf8'), True, False)
        return pprint.PrettyPrinter.format(self, object, context, maxlevels, level)

##
# Simple log
def log( text ):
    print datetime.now().strftime('%Y-%m-%d %H:%m:%I')
    MyPrettyPrinter().pprint(text)
#    print text
    print "\n"

##
# Get list of new transactions from last check
def getFioTransactions( token ):
    try:
        c = httplib.HTTPSConnection("www.fio.cz");
        c.request("GET","/ib_api/rest/last/"+token+"/transactions.xml");
        response = c.getresponse()
        log( str(response.status)+" "+str(response.reason))
        data = response.read()
        return data
    except IOError, e: 
        if hasattr(e, 'code'): # HTTPError
            log( 'http error code: ', e.code )
        elif hasattr(e, 'reason'): # URLError
            log( "can't connect, reason: ", e.reason )
        else: 
            raise

##
# Get list of new transactions from last check
def SetTokenLastLoaded( token , last):
    try:
        c = httplib.HTTPSConnection("www.fio.cz");
        c.request("GET","/ib_api/rest/set-last-id/"+token+"/"+last+"/");
        response = c.getresponse()
        log( str(response.status)+" "+str(response.reason))
        data = response.read()
        return data
    except IOError, e:
        if hasattr(e, 'code'): # HTTPError
            log( 'http error code: ', e.code )
        elif hasattr(e, 'reason'): # URLError
            log( "can't connect, reason: ", e.reason )
        else:
            raise
        
##
# Fake function to simulate receiving list of transactions
def getFioTransactionFake( token ):
    fp = open('test2.xml', "r")
    content = fp.read()
    fp.close()
    return content

##
# Get text field text identified by pattern 'column' in xmltree 'tree'
def getvalue( tree, column ):
    try:
        val = tree.iterfind(column).next()
#        print val.text
        return val.text
    except:
#        print "cannot iterate through"+ column
        return ""
##
# Parse all text in subtree 'Info' in xmltree into dictionary for further use
def getAccountInfo( xmltree ):
    account_info = dict()
    for child in xmltree.iterfind('Info').next():
        account_info[child.tag] = child.text
    return account_info

##
# Check if transaction with passed id was already put into 'prikazy' DB table
def getIfPrikazExist( tr_id ):
    q = "SELECT count(*) cnt FROM prikazy WHERE bankovni_reference = %s" % (tr_id)
    try:
        cursor.execute(q)
        row = cursor.fetchone()
        if float(row['cnt'] > 0):
            return True
        else:
            return False
    except MySQLdb.Error, e:
        try:
            log( "MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
            log(q)
        except IndexError:
            log( "MySQL Error: %s" % str(e))
            log(q)
        return False

##
# Fill struct of table 'prikazy' by one transaction
# Mapping XML to db table
def fillPrikazy( trans, acc_info ):
    prikazy = dict()
    # Own account info
    prikazy['idimport'] = 0; #TODO solve
    prikazy['cislo_uctu'] = acc_info['accountId']
    prikazy['iban'] = acc_info['iban']
    if acc_info.has_key('idList'):
        prikazy['cislo_vypisu'] = acc_info['idList']
    else:
        prikazy['cislo_vypisu'] = 0;
#    prikazy[''] = acc_info['']

    # transaction info
    prikazy['odeslano'] = getvalue(trans, 'column_0')
    prikazy['splatnost'] = getvalue(trans, 'column_0')
    prikazy['castka'] = getvalue(trans, 'column_1')
    prikazy['mena'] = getvalue(trans, 'column_14')
    prikazy['typ_transakce'] = getvalue(trans, 'column_8')
    prikazy['cislo_transakce'] = getvalue(trans, 'column_22')
    prikazy['bankovni_reference'] = getvalue(trans, 'column_22') # TODO Check bankovni reference
#    prikazy['reference_klienta'] = getvalue(trans, '')
    prikazy['cislo_protiuctu'] = getvalue(trans, 'column_2')
    prikazy['nazev_protiuctu'] = getvalue(trans, 'column_10')
    prikazy['banka_protiuctu'] = getvalue(trans, 'column_3')
    prikazy['konstantni_symbol'] = getvalue(trans, 'column_4')
    prikazy['variabilni_symbol'] = getvalue(trans, 'column_5')
    prikazy['specificky_symbol'] = getvalue(trans, 'column_6')
#popis1
#popis2
#popis3
    prikazy['zprava'] = getvalue(trans, 'column_16')
    prikazy['poznamka'] = getvalue(trans, 'column_25')
#    prikazy['kategorie'] = getvalue(trans, '')
#    prikazy['pouzito'] = getvalue(trans, '')

    return prikazy
##
# Mappping table 'prikazy' to table 'lide_platby'
def fillLidePlatby( prikaz, lide ):
    pl = dict()
    pl['idlide'] = lide['idlide']
    pl['idnody'] = lide['idnody']
   
    pl['idplatby'] = 4 # Dont know why
    pl['idprikazy'] = prikaz['idprikazy']
    pl['idimport'] = prikaz['idimport']
    pl['zaplatil'] = prikaz['splatnost']
    pl['melplatit'] = prikaz['splatnost']
#    pl['poznamka'] = prikaz['']
    pl['nazev_uctu'] = prikaz['nazev_protiuctu']
    pl['cislo_uctu'] = prikaz['cislo_protiuctu']
    pl['kod_banka'] = prikaz['banka_protiuctu']
    pl['vs'] = prikaz['variabilni_symbol']
    return pl

##
# mapping tables 'prikazy' + 'transakce' to 'prikazy_transakce'
def fillPrikazyTransakce(prik_trans):
    pt = dict()
    pt['idprikazy'] = prik_trans['idprikazy'];
    pt['idimport'] = prik_trans['idimport'];
    pt['idtransakce'] = prik_trans['idtransakce'];
    pt['datum'] = prik_trans['splatnost'];
    pt['castka'] = prik_trans['castka'];
    return pt


##
# Handle misc payments which does not belong to users
def processSystemSpecificTransactions():
    sql = (' SELECT p.idimport, p.castka, '
                  'p.splatnost, p.idprikazy, '
                 ' p.typ_transakce, t.idtransakce '
             'FROM prikazy p, transakce t  '
             'WHERE pouzito=0 AND '
                   'p.typ_transakce = t.nazev')
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        try:
            log( "MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
            log(q)
        except IndexError:
            log( "MySQL Error: %s" % str(e))
            log(sql)
        return False
    for row in rows:
       pt = fillPrikazyTransakce(row)
       ret = mysqlDictInsert("INSERT INTO prikazy_transakce (%s) VALUES (%s)", pt)
       if ret == 1062:
           log('Duplicit transaction. Removing.')
           removeFromPrikazy(pt['idprikazy'])
           continue;
       # Paired!
       if ret == True:
           setPrikazAsPaired(pt['idprikazy'])
           continue
       log("Unknown error on pairing transaction!")


##
# Process one transaction - insert it into DB
def processTransaction( trans ):
    # fill struct of db prikaz table
    prikaz = fillPrikazy(trans, account_info)

    if getIfPrikazExist(prikaz['bankovni_reference']):
        log( "Not inserting. Transaction %s already exists" % prikaz['bankovni_reference'])
        log( prikaz )
        return False
    else :
        return mysqlDictInsert("INSERT INTO prikazy (%s) VALUES (%s)", prikaz)

##
# Go through all unpaired transactions
def processPrikazy():
#    sql = "SELECT idimport,castka,variabilni_symbol,nazev_protiuctu,cislo_protiuctu,banka_protiuctu,splatnost,idprikazy FROM prikazy WHERE pouzito=0 AND typ_transakce NOT IN (SELECT nazev FROM transakce)";
    sql = ('SELECT idimport, castka, variabilni_symbol, nazev_protiuctu, '
                   'cislo_protiuctu, banka_protiuctu, splatnost, idprikazy '
               'FROM prikazy '
               'WHERE pouzito=0 AND idimport = 0 AND '
                     'typ_transakce NOT IN (SELECT nazev FROM transakce)')
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        try:
            log( "MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
            log(q)
        except IndexError:
            log( "MySQL Error: %s" % str(e))
            log(sql)
        return False
    for row in rows:
        pairTransaction(row)

##
# Try to pair one transaction
def pairTransaction( prikaz ):
    # Do not pair outgoing payments
    if prikaz['castka'] < 0:
        log("Not pairing outgoing payments")
        log(prikaz)
        return False

    # Payments without VS are not paired here
    if len(prikaz['variabilni_symbol']) == 0:
        log("Missing variable symbol!")
        log(prikaz)
        return False

    # Get user by VS
    user = getUserByVarSymbol( prikaz['variabilni_symbol'])
    if user == False :
        log("Cannot find user for variable symbol %s" % prikaz['variabilni_symbol'])
        log(prikaz)
        return False

    # Fill data to dict
    l_platba = fillLidePlatby( prikaz, user)
    # Insert data
    ret = mysqlDictInsert("INSERT INTO lide_platby (%s) VALUES (%s)", l_platba)
    # There is already record with that ID. Remove transaction.
    if ret == 1062:
        log('Duplicit transaction. Removing.')
        removeFromPrikazy(prikaz['idprikazy'])
        return False;
    # Paired!
    if ret == True:
        setPrikazAsPaired(prikaz['idprikazy'])
        return True
    log("Unknown error on pairing transaction!")
    return False

def setPrikazAsPaired( idprikazy ):
    sql = "UPDATE prikazy SET pouzito=1 WHERE idprikazy='%s'" % str(idprikazy)
    try:
        cursor.execute(sql)
        return True
    except IndexError:
        log( "MySQL Error: %s" % str(e))
        log(sql)
    return False

def removeFromPrikazy( idprikazy ):
    sql = "DELETE FROM prikazy WHERE idprikazy='%s'" % str(idprikazy)
    try:
        cursor.execute(sql)
        return True
    except IndexError:
        log( "MySQL Error: %s" % str(e))
        log(sql)
    return False
    
##
# Get user id by variable symbol
def getUserByVarSymbol( vs ):
    vspad =  "%010d" % (int(vs))
    sql = "SELECT idlide, nick, idnody, vs FROM lide WHERE vs='%s'" % vspad
    try:
        cursor.execute(sql)
        if cursor.rowcount == 0:
            return False
        row = cursor.fetchone()
        return row
    except IndexError:
        log( "MySQL Error: %s" % str(e))
        log(sql)    
    return False
    
##
# Go through all transactions
def processTransactions( xmltree ):
    inserted = 0
    for trans in xmltree.iterfind('TransactionList/Transaction'):
        if processTransaction(trans) == True:
           inserted = inserted + 1

    log("Inserted %s transactioncs into 'prikazy'" % inserted)
    return inserted
        

##
# Process insert of passed 'sql' with keys and values passed as dictionary
def mysqlDictInsert( sql, data):
    # Prepare '%s, %s, ...' based on length of data
    placeholders = ', '.join(['%s'] * len(data))
    # fill ', ' separated list of columns from dictionary
    columns = ', '.join(data.keys())
    # fill columns and placeholders into sql
    querry = sql % ( columns, placeholders)
    try:
#        print querry
#        print data
        cursor.execute(querry, data.values())
        db.commit()
        return True
    except MySQLdb.Error, e:
        try:
            log( "MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
            log(querry)
            log(data.values())
            return e.args[0]
        except IndexError:
            log( "MySQL Error: %s" % str(e))
            log(querry)
            log(data.values())
        db.rollback()
        return e


####################################
## Main script
####################################

# Read configuration file
config = ET.parse(config_file)
token = getvalue(config, "BankToken")
db_host = getvalue(config, "db/host")
db_user = getvalue(config, "db/username")
db_pass = getvalue(config, "db/password")
db_name = getvalue(config, "db/name")

# Set token TODO remove!!!
#print(SetTokenLastLoaded(token, "9269249320"))

# Get list of transactions 
#xmldata = getFioTransactionFake(token)
xmldata = getFioTransactions(token)
# Parse XML
xmltree = ET.fromstring(xmldata)

# Get information about account and list
account_info = getAccountInfo(xmltree)
log(account_info)

# Check if there os something to process
#if sum(1 for i in xmltree.iterfind('TransactionList/Transaction')) <= 0:
#    quit()

# Open database connection
#db = MySQLdb.connect(db_host,db_user,db_pass,db_name, use_unicode=True, charset="utf8" )
db = MySQLdb.connect(db_host,db_user,db_pass,db_name )
# prepare a cursor object using cursor() method
cursor = db.cursor(MySQLdb.cursors.DictCursor)
#cursor.execute("SET NAMES utf8mb4;") #or utf8 or any other charset you want to handle
#cursor.execute("SET CHARACTER SET utf8mb4;") #same as above
#cursor.execute("SET character_set_connection=utf8mb4;")

#if processTransactions(xmltree) > 0:
log("Processing records in 'prikazy'")
processPrikazy()

log("Processing System specific transaction")
processSystemSpecificTransactions()
 

# disconnect from server
db.close()




