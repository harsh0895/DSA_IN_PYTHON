import psycopg2 as py
import pandas as pd

connection = py.connect(database="mtn", user="fagunt", password="bvw!vbx3egd*uju9WVM",
                        host="mtn-pg-test.ch8duojypcpv.us-west-2.rds.amazonaws.com", port=5432,options="-c search_path=mtn-depot")

cursor = connection.cursor()

# Fetch all rows from database
sql_insert_query="insert into mtn_depot.temp_pop_stats_2024_06 values("
para= "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
for i in range(23):
        #print(i)
    sql_insert_query = sql_insert_query+para
    
sql_insert_query=sql_insert_query+"%s)"
print(sql_insert_query)
df = pd.read_csv("PS_24_01_BG.csv")
i=0
for index, row in df.iterrows():
    arr=[]
    print(row['Q001001'])
    arr.extend([int(row['FIPS']), str(row['LONG']),str(row['LAT']),str(row['AREALAND']),str(row['AREAWATR']),int(row['Q001001']),int(row['Q001CG1']),int(row['Q001CH1']),int(row['Q001CI1']),int(row['Q001CJ1'])])
    arr.extend([int(row['Q001CK1']),int(row['Q001CL1']),int(row['Q001CM1']),int(row['Q001CN1']),int(row['Q002002']),int(row['Q002CG2']),int(row['Q002CH2']),int(row['Q002CI2']),int(row['Q002CJ2']),int(row['Q002CK2'])])
    arr.extend([int(row['Q002CL2']),int(row['Q002CM2']),int(row['Q002CN2']),int(row['Q003001']),int(row['Q003002']),int(row['Q003003']),int(row['Q004001']),int(row['Q004CG1']),int(row['Q004CH1']),int(row['Q004CI1'])])
    arr.extend([int(row['Q004CJ1']),int(row['Q004CK1']),int(row['Q004CL1']),int(row['Q004CM1']),int(row['Q004CN1']),int(row['Q008003']),int(row['Q008004']),int(row['Q008006']),int(row['Q008010']),int(row['Q008099'])])
    arr.extend([int(row['Q009007']),int(row['Q009008']),int(row['Q009010']),int(row['Q009111']),int(row['Q009112']),int(row['Q009113']),int(row['Q009114']),int(row['Q009120']),int(row['Q009130']),int(row['Q009199'])])
    
    arr.extend([int(row['Q009211']),int(row['Q009212']),int(row['Q009213']),int(row['Q009220']),int(row['Q009230']),int(row['Q009299']),int(row['Q009410']),int(row['Q009420']),int(row['Q009430']),int(row['Q009440'])])
    arr.extend([int(row['Q009450']),int(row['Q009460']),int(row['Q009470']),int(row['Q009480']),int(row['Q009499']),int(row['Q009501']),int(row['Q009601']),int(row['Q009701']),str(row['Q00X001']),str(row['Q00X002'])])
    arr.extend([str(row['Q00X004']),str(row['Q00X005']),int(row['Q010003']),int(row['Q010004']),int(row['Q010025']),int(row['Q010027']),int(row['Q010032']),int(row['Q010033']),int(row['Q010034']),int(row['Q012002'])])
    arr.extend([int(row['Q012003']),int(row['Q012004']),int(row['Q012006']),int(row['Q012007']),int(row['Q012009']),int(row['Q012011']),int(row['Q012012']),int(row['Q012013']),int(row['Q012014']),int(row['Q012015'])])
    arr.extend([int(row['Q012016']),int(row['Q012017']),int(row['Q012018']),int(row['Q012020']),int(row['Q012022']),int(row['Q012023']),int(row['Q012024']),int(row['Q012025']),int(row['Q012026']),int(row['Q012027'])])

    arr.extend([int(row['Q012028']),int(row['Q012030']),int(row['Q012031']),int(row['Q012033']),int(row['Q012035']),int(row['Q012036']),int(row['Q012037']),int(row['Q012038']),int(row['Q012039']),int(row['Q012040'])])
    arr.extend([int(row['Q012041']),int(row['Q012042']),int(row['Q012044']),int(row['Q012046']),int(row['Q012047']),int(row['Q012048']),int(row['Q012049']),str(row['Q013A01']),str(row['Q013B01']),int(row['Q014010'])])
    arr.extend([int(row['Q014011']),int(row['Q014012']),int(row['Q014013']),int(row['Q014014']),int(row['Q014015']),int(row['Q014016']),int(row['Q015001']),int(row['Q015CG1']),int(row['Q015CH1']),int(row['Q015CI1'])])
    arr.extend([int(row['Q015CJ1']),int(row['Q015CK1']),int(row['Q015CL1']),int(row['Q015CM1']),int(row['Q015CN1']),str(row['Q017001']),int(row['Q035003']),int(row['Q035007']),int(row['Q035011']),int(row['Q035012'])])
    arr.extend([int(row['Q035014']),int(row['Q035015']),int(row['Q035016']),int(row['Q035017']),int(row['Q035018']),int(row['Q037002']),int(row['Q037006']),int(row['Q037007']),int(row['Q037008']),int(row['Q043006'])])

    arr.extend([str(row['Q043X09']),int(row['Q052002']),int(row['Q052003']),int(row['Q052004']),int(row['Q052005']),int(row['Q052006']),int(row['Q052007']),int(row['Q052008']),int(row['Q052009']),int(row['Q052010'])])
    arr.extend([int(row['Q052014']),int(row['Q052015']),int(row['Q052016']),int(row['Q052017']),int(row['Q052018']),int(row['Q052019']),int(row['Q052020']),int(row['Q052A11']),int(row['Q052A12']),int(row['Q052A13'])])
    arr.extend([int(row['Q052B11']),int(row['Q052B12']),int(row['Q052B13']),int(row['Q052C12']),int(row['Q052C13']),int(row['Q052D13']),int(row['Q052E13']),int(row['Q053X01']),int(row['Q054X01']),int(row['Q083X01'])])
    arr.extend([int(row['Q084001']),int(row['Q085X01']),int(row['Q086X01']),int(row['Q087X98']),int(row['Q087X99']),int(row['Q089002']),int(row['Q093X01']),int(row['Q094X01']),int(row['R001001']),int(row['R0010H1'])])
    arr.extend([int(row['R0010L1']),int(row['R008003']),int(row['R008004']),int(row['R008006']),int(row['R008010']),int(row['R008099']),int(row['R012002']),int(row['R012026']),str(row['R013A01']),str(row['R013B01'])])

    arr.extend([int(row['R015001']),str(row['R017001']),int(row['R037001']),int(row['R053X01']),int(row['R054X01']),int(row['S001001']),int(row['S012002']),int(row['S012026']),str(row['S013A01']),str(row['S013B01'])])
    arr.extend([int(row['S015001']),int(row['S016001']),str(row['S017001']),int(row['S037001']),int(row['VC00000']),int(row['VC0000A']),int(row['VC0000B']),int(row['VC0000C']),int(row['VC0000D']),int(row['VC0000E'])])
    arr.extend([int(row['VC0000F']),int(row['VC0000G']),int(row['VC0000H']),int(row['VC0000I']),int(row['VC0000J']),int(row['VC0000K']),int(row['VC0000L']),int(row['VC0000M']),int(row['VC0000N']),int(row['VC0000O'])])
    arr.extend([str(row['Z086X01'])])
    print("------------------------",len(arr))
    cursor.execute(sql_insert_query,tuple(arr))
    connection.commit()
    i+=1
    if(i==10000):
        break
connection.close()

