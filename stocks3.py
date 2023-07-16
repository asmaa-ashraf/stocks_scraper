from selenium.webdriver.common.by import By
from datetime import timedelta
from selenium import webdriver
import pandas as pd
from datetime import datetime
import yfinance as yf
from zipfile import ZipFile
import os,sys
from time import sleep
import numpy as np
from selenium.webdriver.support.ui import WebDriverWait
err_msg=''
folder='stocks data'
if not os.path.exists(folder):
    os.makedirs(folder)
#start the scraping
names=[]
sym_nme = pd.read_excel('symbol_names.xlsx')
symbols = sym_nme['Symbol']
def ambersand(x):
    if isinstance(x,float) or isinstance(x,int):
        return str(x)+'%'
    else:
        return ''
def get_rev_url(symbol):
    global sym_nme
    try:
        name=sym_nme.loc[sym_nme['Symbol']==symbol]['Name'].tolist()
        name=name[0]
    except Exception as e:
        #print(e)
        #print(symbol)
        return False
    if '\n' in name:
        name=name[:-2]
    #print(name)
    if 'http' in name:
        url=name
    else:
        url='https://in.investing.com/equities/'+name+'-historical-data-earnings'
    return url
def get_revenue_url(nme,driver):
    global err_msg
    try:
        stock_url='https://in.investing.com/search/?q='+nme
        driver1.get(stock_url)
        #the info we want from the url are the dates
        #then the time should be subtracted from it to create anither column specifying bmo :before market open,mo and amo
        #the dates wanted start from 2010 to now
        div=driver1.find_element(By.XPATH,'//div[@class="js-inner-search-results-wrapper common-table medium"]')
        #print('div rev',div)
        a=div.find_element(By.XPATH , './/a')
        #print('a rev',a)
        name=a.get_attribute('href')
        if name:
            name = name.split('/')[-1]
            names.append(name)
        #print('name_rev',name)
        stock_url='https://in.investing.com/equities/'+name+'-historical-data-earnings'
    except Exception as e:
        print('get_revenue_url function ERROR')
        err_msg=err_msg+'\n'+str(e)+str(sys.exc_info()[-1].tb_lineno)

        print(e)
    return stock_url

def get_revenue(url,driver):
    #print('getting revenue from', url)
    global err_msg
    try:
        driver1.get(url)
        sleep(5)
        rows = driver1.find_element(By.XPATH,'//tr')
        #print('rows', rows)
        date = rows.find_elements(By.XPATH, '//td[contains(@class,"col-release_date")]')
        dts = [x.text for x in date]
        dates=[]
        for d in dts :
            if d!='' :
                date=datetime.strptime(d,'%b %d, %Y')
                if date.year>2010 and date<=datetime.now():
                    date=date.date()
                    dates.append(date)
                else:
                    print('rev dates skip',date)
        #print('schedule_rev', dates)
        #print(len(dates))
        rvn = rows.find_elements(By.XPATH, '//td[contains(@class,"col-revenue_surprise")]')
        revenue_surprise = [x.text for x in rvn]
        #print('revenue', revenue_surprise)
        #print('len1',len(revenue_surprise))
        revenue_surprise=revenue_surprise[:len(dates)]
        #print('len2',len(revenue_surprise))
        revenue=[r for r in revenue_surprise]
        revenue_fr = pd.DataFrame(columns=['Revenue', 'Dates'])
        revenue_fr['Dates'] = dates
        revenue_fr['Revenue'] = revenue
        #print('end get rev func')
    except Exception as e:
        err_msg=err_msg+'\n'+str(e)
        print('get_revenue func ERROR')
        print(e)
        # str(sys.exc_info()[-1].tb_lineno)

    return revenue_fr

def summ(col_name):
    index = 0
    if "egative" in col_name:
        op = '-'
    else:
        op = '+'
    sum0, sum1, sum2, sum3, sum4, sum5 = 0, 0, 0, 0, 0, 0
    no_events=0
    #print('main_frame.columns',main_frame.columns)
    for sur in main_frame[col_name]:
        if not index<len(main_frame['T=0 Return']):
            break
        if sur == 1:
            sum0=calculate(sum0,main_frame['T=0 Return'].iloc[index][:-1],op)
            sum1 = calculate(sum1,main_frame['T+1 Return'].iloc[index][:-1],op)
            sum2 = calculate(sum2,main_frame['T+2 Return'].iloc[index][:-1],op)
            sum3 = calculate(sum3,main_frame['T+3 Return'].iloc[index][:-1],op)
            sum4 = calculate(sum4,main_frame['T+4 Return'].iloc[index][:-1],op)
            sum5 = calculate(sum5,main_frame['T+5 Return'].iloc[index][:-1],op)
            no_events += 1
        index += 1
    #print('no events', no_events, col_name)
    if no_events!=0:
        sum = [sum0, sum1, sum2, sum3, sum4, sum5]
        return_per_day_event = [calculate(x,no_events,'/') for x in sum ]
    else:
        sum = ['-', '-', '-', '-', '-', '-']
        return_per_day_event = ['-' for x in sum]
    return sum,no_events,return_per_day_event


def convert2float(x):
    try:
        r= float(x)
    except:
        r= ''
    return r

def calculate(x,y,op):
    #print('x:',x,'y:',y)
    r=0
    if x=='-' or y=='-':
        r='-'
    try:
        x=float(x)
    except:
        #x=1 if op=='*' or op=='/' else 0 # it won't make much differnce in the output if the data was missing
        r='-'
    try:
        y=float(y)
    except:
        r= '-'
        #y=1 if op=='*' or op=='/' else 0
    if r==0:
        if op=='+':
            r= x+y
        elif op=='-':
            r= x-y
        elif op=='*':
            r= x*y
        elif op=='/' :
            if y==0:
                #print('division by 0')
                r='-'
            else:
                r= x/y
        else:
            r='-'
            #print(op,'unsupported operand')
    #print(r)
    return r



def get_history(ticker, date):
    # try:
    history_dates = {}
    dates = ['T=0', 'T-1', 'T-2', 'T-3', 'T-4', 'T-5', 'T+1', 'T+2', 'T+3', 'T+4', 'T+5']
    history = ticker.history(start=date - timedelta(days=10), end=date + timedelta(days=10))
    if history.empty:
        data = 'not available'
        return ''
    history = history.reset_index()
    # Rename old index from '' to Date
    history.columns = ['Date', *history.columns[1:]]
    history['Date'] = history['Date'].dt.date
    i = history.index[history['Date'] == date.date()].tolist()
    if len(i)<1:
        #print(history)
        return ''
    i = i[0]
    for d in dates:
        if '=' in d:
            z = 0
        else:
            z = int(d[1:])
        try:
            history_dates[d] = history.iloc[i + z]
            history_dates[d]['Date']=history_dates[d]['Date'].strftime("%d-%b-%y")
        except:
            history_dates[d] = {'Close':'-','Date':'-','Open':'-','High':'-','Low':'-',}
    # except Exception as e:
    #     #print('get history func ERROR')
    #     err_msg=err_msg+'\n'+str(e)
    #     #print(e)
    #     str(sys.exc_info()[-1].tb_lineno)

    return history_dates

    # try:
    history_dates = {}
    dates = ['T=0', 'T-1', 'T-2', 'T-3', 'T-4', 'T-5', 'T+1', 'T+2', 'T+3', 'T+4', 'T+5']
    history = ticker.history(start=date - timedelta(days=20), end=date + timedelta(days=30))
    if history.empty:
        data = 'not available'
        return ''
    history = history.reset_index()
    # Rename old index from '' to Date
    history.columns = ['Date', *history.columns[1:]]
    history['Date'] = history['Date'].dt.date
    i = history.index[history['Date'] == date.date()].tolist()
    if len(i)<1:
        #print(history)
        return ''
    i = i[0]
    for d in dates:
        if '=' in d:
            z = 0
        else:
            z = int(d[1:])

        try:
            history_dates[d] = history.iloc[i + z]
        except:
            history_dates[d] = ''
    # except Exception as e:
    #     #print('get history func ERROR')
    #err_msg = err_msg + '\n' + 'get_history_func'+str(e)

    #     #print(e)
    #     str(sys.exc_info()[-1].tb_lineno)

    return history_dates



main_cols = ['Date', 'Time', 'Period',  # all from the date we get from the site
        'EPS Estimate', 'EPS Actual', 'EPS Surprise',
        'Revenue Surprise', 'Drift Since Prev.Earnings',
        'T=0 Earnings Date', 'T=0 Open', 'T=0 Close', 'T-1 Date', 'T-2 Date', 'T-1 Close', 'T-2 Close',
        'T-1 price change', 'T+1 Date',
        'T+2 Date', 'T+3 Date', 'T+4 Date', 'T+5 Date', 'T+1 Close',
        'T+2 Close', 'T+3 Close', 'T+4 Close', 'T+5 Close', 'T=0 Return', 'T+1 Return',
        'T+2 Return', 'T+3 Return', 'T+4 Return', 'T+5 Return', 'T=0 Return/day', 'T+1 Return/day',
        'T+2 Return/day', 'T+3 Return/day', 'T+4 Return/day', 'T+5 Return/day', 'Positive EPS Surprise',
        'Positive Rev. surprise'
    , 'Negative Price drift', 'Positive Price drift', 'Negative T-1 price change', 'Positive T-1 price change']
try:
    finished_list=pd.read_excel('finished.xlsx')['Symbol'].tolist()
except:
    finished_list=[]
    with open('finished.csv','w') as f:
        f.write('')
new_i=0
explicit_wait = 15
driver = webdriver.Chrome()
driver1 = webdriver.Chrome()

wait = WebDriverWait(driver, explicit_wait)
for symbol in symbols:
    # try:

    err_msg=''
    if symbol in finished_list:
        #print(symbol,' is already done')
        continue
    stocks_url = 'https://finance.yahoo.com/calendar/earnings/?symbol=' + symbol
    print(stocks_url)
    driver.get(stocks_url)
    case_index1,case_index2=0,0
    # the info we want from the url are the dates
    # then the time should be subtracted from it to create anither column specifying bmo :before market open,mo and amo
    # the dates wanted start from 2010 to now
    main_frame = pd.DataFrame(columns=main_cols)
    rows = driver.find_element(By.XPATH, '//tr')
    #print('rows', rows)
    date_time = rows.find_elements(By.XPATH, '//td[@aria-label="Earnings Date"]')
    datetimes = [x.text for x in date_time]
    #print('sched', datetimes)
    r = 0
    company = rows.find_element(By.XPATH, '//td[@aria-label="Company"]').text
    n = get_rev_url(symbol)
    #print(n)
    if not n:
        revenue_url = get_revenue_url(company, driver)
    else:
        revenue_url = n

    EPS = rows.find_elements(By.XPATH, '//td[@aria-label="EPS Estimate"]')
    EPS_estimate = [convert2float(x.text) for x in EPS]
    #print('eps estimate', EPS_estimate)
    reportedEPS = rows.find_elements(By.XPATH, '//td[@aria-label="Reported EPS"]')
    reported_EPS = [convert2float(x.text) for x in reportedEPS]
    #print('reported eps', reported_EPS)
    surprise = rows.find_elements(By.XPATH, '//td[@aria-label="Surprise(%)"]')
    surprises = [convert2float(x.text) for x in surprise]
    #print('surprise', surprises)
    if not n:
        revenue_url = get_revenue_url(company, driver)
    else:
        revenue_url = n
    try:
        revenue_fr = get_revenue(revenue_url, driver)
    except Exception as e:
        print(e)
        err_msg=err_msg+'\n'+'getting revenue'+str(e)
        print('no revenue data ERROR continue')
        continue
    new_cols = {'Earnings Date': 'T=0 Earnings Date', 'Time': 'Time', 'T': 'Date', 'C(T-1)': 'T-1 Close',
                'O(T)': 'T=0 Open', 'H(T)': '',
                'L(T)': '', 'C(T)': 'T=0 Close', 'C(T+1)': 'T+1 Close', 'Order Fulfilment': '',
                'T=0 Return(0)': '', 'T=1 Return(0)': '', 'Buying price(0)': '',
                'T=0 Return(1)': '', 'T=1 Return(1)': '', 'Buying price(1)': ''}
    cases_cols = list(new_cols.keys())

    NPDFrame = pd.DataFrame(columns=cases_cols)
    NTPCFrame = pd.DataFrame(columns=cases_cols)
    for d in datetimes:
        o_d=d
        d=datetime.strptime(d[:-3], '%b %d, %Y, %I %p')
        if d.year < 2010 or d.now() < d:
            r += 1
            continue
        # get the time separated and classify it
        # convert the dates to be like those in the alibaba sheet
        ticker = yf.Ticker(symbol)
        history = get_history(ticker, d)
        if history=='':
            r += 1
            continue
        date = d
        ind= datetimes.index(o_d)
        if ind>=len(datetimes)-1:
            prev_history='-'
            next_date='-'
            #print('no date bef',d)

        else:
            next_date = datetime.strptime(datetimes[ind+ 1][:-3], '%b %d, %Y, %I %p')
            prev_history = get_history(ticker, next_date)['T=0']['Close']
            #print(next_date,'next date')
        #print(prev_history, 'prev hist')
        time = d.hour
        t = ''
        if time < 9:
            t = 'BMO'
        elif time > 9 and time < 17:
            t = "MO"
        else:
            t = "AMC"
        month = d.month
        period = ''
        year = str(d.year)
        if month >= 1 and month <= 3:
            period = year + ' Q3'
        elif month >= 4 and month <= 6:
            period = year + ' Q4'
        elif month >= 7 and month <= 9:
            period = str(d.year + 1) + ' Q1'
        else:
            period = str(d.year + 1) + ' Q2'
        if t == 'AMC':
            original_d = d
            d = d + timedelta(days=1)
            original_date = date
            date = d.strftime("%d-%b-%y")
        else:
            date = d.strftime("%d-%b-%y")
            original_d = d
            original_date = date
        #print('t-1 close',history['T-1']['Close'],history['T-1']['Date'])
        #drift1=history['T-1']['Close']/prev_history
        drift1= calculate(calculate(history['T-1']['Close'] , prev_history,op='/') , 1,'-')
        drift=calculate(drift1 ,100,'*')
        #print(drift,'drift')
        price_change=calculate((calculate(history['T-1']['Close'],history['T-2']['Close'] ,'/')-1),100,'*')
        n = get_rev_url(symbol)
        #print(n)

        try:
            revenue=revenue_fr.loc[revenue_fr['Dates']==d.date()]['Revenue']
            revenue=revenue.iloc[0]
            #print('revenue',revenue)

        except Exception as e :
            #print('no rev for date',d)
            #print(e)
            revenue='-'
        #print('###################################################')
        hc5=history['T+5']['Close']
        ho0=history['T=0']['Open']
        t51=calculate(hc5,ho0,'/')
        t50=calculate(t51 ,1,'-')
        t5=calculate( t50, 100,'*')
        #print(t5,'t5')
        calculate(calculate(calculate(history['T+5']['Close'], history['T=0']['Open'], '/'), 1, '-'), 100, '*')
        returnT= {'T=0':calculate(calculate(calculate(history['T=0']['Close'] , history['T=0']['Open'],'/') ,1,'-') , 100,'*'),#C(T)/O(T)
                  'T+1':calculate(calculate(calculate(history['T+1']['Close'] ,history['T=0']['Open'],'/') ,1,'-') , 100,'*'),
                  'T+2':calculate(calculate(calculate(history['T+2']['Close'] ,history['T=0']['Open'],'/') ,1,'-'), 100,'*'),
                  'T+3':calculate(calculate(calculate(history['T+3']['Close'] ,history['T=0']['Open'],'/') ,1,'-') , 100,'*'),
                  'T+4':calculate(calculate(calculate(history['T+4']['Close'] ,history['T=0']['Open'],'/') ,1,'-'), 100,'*'),
                  'T+5':t5}
        try:
            Positive_EPS_Surprise= 1 if isinstance(surprises[r],float) and surprises[r]> 1 else 0
        except:
            Positive_EPS_Surprise=''
        try:
            Positive_Rev_surprise = 1 if convert2float(surprises[r]) > 1 and convert2float(revenue) > 0 else 0
        except:
            Positive_Rev_surprise =''
        try:
            negative_price_drift=1 if Positive_EPS_Surprise and drift<0 else 0
        except:
            negative_price_drift =''
        try:
            Positive_Price_drift = 1 if Positive_EPS_Surprise and drift>0 else 0
        except:
            Positive_Price_drift =''
        try:
            Negative_price_change=1 if Positive_EPS_Surprise and price_change<0 else 0
        except:
            Negative_price_change = ''
        try:
            Positive_price_change=1 if Positive_EPS_Surprise and price_change>0 else 0
        except:
            Positive_price_change=''
        row = {'Date': original_date, 'Time': t, 'Period': period,  # all from the date we get from the site #ABC
               'EPS Estimate': EPS_estimate[r],  # E
               'EPS Actual': reported_EPS[r],  # F
               'EPS Surprise': surprises[r],  # G
               'Revenue Surprise': revenue,  # J
               'Drift Since Prev.Earnings': drift ,  # K
               'T=0 Earnings Date': date,  # M
               'T=0 Open': history['T=0']['Open'],  # N
               'T=0 Close': history['T=0']['Close'],  # O
               'T-1 Date': history['T-1']['Date'],  # P
               'T-2 Date': history['T-2']['Date'],  # Q
               'T-1 Close': history['T-1']['Close'],  # R
               'T-2 Close': history['T-2']['Close'],  # S
               'T-1 price change': price_change,
               'T+1 Date': history['T+1']['Date'],  # U
               'T+2 Date': history['T+2']['Date'],  # V
               'T+3 Date': history['T+3']['Date'],  # W
               'T+4 Date': history['T+4']['Date'],  # X
               'T+5 Date': history['T+5']['Date'],
               'T+1 Close': history['T+1']['Close'],
               'T+2 Close': history['T+2']['Close'],
               'T+3 Close': history['T+3']['Close'],
               'T+4 Close': history['T+4']['Close'],
               'T+5 Close': history['T+5']['Close'],
               'T=0 Return': str(returnT['T=0']) + '%',
               'T+1 Return': str(returnT['T+1']) + '%',
               'T+2 Return': str(returnT['T+2']) + '%',
               'T+3 Return': str(returnT['T+3']) + '%',
               'T+4 Return': str(returnT['T+4']) + '%',
               'T+5 Return': str(returnT['T+5']) + '%',
               'T=0 Return/day': str(returnT['T=0']) + '%',
               'T+1 Return/day': str(calculate(calculate(returnT['T+1'] , 2,'/') ,100,'*')) + '%',
               'T+2 Return/day': str(calculate(calculate(returnT['T+2'] , 3,'/') ,100,'*')) + '%',
               'T+3 Return/day': str(calculate(calculate(returnT['T+3']  , 4,'/') ,100,'*'))+ '%',
               'T+4 Return/day': str(calculate(calculate(returnT['T+4'] , 5,'/') ,100,'*'))+ '%',
               'T+5 Return/day': str(calculate(calculate(returnT['T+5']  , 6,'/') ,100,'*'))+ '%',
               'Positive EPS Surprise': Positive_EPS_Surprise,
               'Positive Rev. surprise': Positive_Rev_surprise,
               'Negative Price drift':negative_price_drift,
               'Positive Price drift': Positive_Price_drift,
               'Negative T-1 price change': Negative_price_change,
               'Positive T-1 price change': Positive_price_change}
        main_frame.loc[new_i] = row
        r += 1
        new_i +=1
        #sheet2 calculations

        case_row=new_cols
        print('case row',case_row)
        #first table
        case={'Negative Price drift':False,'Negative T-1 price change':False}
        if negative_price_drift:
            case['Negative Price drift']= True
        if Negative_price_change:
            case['Negative T-1 price change']=True

        order_fulfilment,buying_price,buying_price1,TR0,TR1=False,0,0,0,0
        if case['Negative Price drift'] or case['Negative T-1 price change']:
            for col in cases_cols:
                print(col)
                print('new_cols',new_cols)
                print(new_cols[col])
                new_cols = {'Earnings Date': 'T=0 Earnings Date', 'Time': 'Time', 'T': 'Date', 'C(T-1)': 'T-1 Close',
                            'O(T)': 'T=0 Open', 'H(T)': '',
                            'L(T)': '', 'C(T)': 'T=0 Close', 'C(T+1)': 'T+1 Close', 'Order Fulfilment': '',
                            'T=0 Return(0)': '', 'T=1 Return(0)': '', 'Buying price(0)': '',
                            'T=0 Return(1)': '', 'T=1 Return(1)': '', 'Buying price(1)': ''}
                if new_cols[col] != '':
                    case_row[col] = row[new_cols[col]]
                elif col=='H(T)':
                    case_row[col] = history['T=0']['High']
                    print('high',history['T=0']['High'])
                elif col=='L(T)':
                    case_row[col] = history['T=0']['Low']
                    print('low',history['T=0']['Low'])

                # order_fulfilment=1 if 'C(T)'>'H(T)' else ''
                elif col=='Order Fulfilment':
                    order_fulfilment= 1 if history['T=0']['High']<history['T=0']['Close'] else ''
                    case_row[col] = order_fulfilment
                # Treturn0=('c(T)'/'O(T)'-1)*100+'%' if order_fulfilment else ''
                #returnT= {'T=0':(calculate(history['T=0']['Close'] , history['T=0']['Open'],'/') - 1) * 100,#C(T)/O(T)
                elif col=='T=0 Return(0)':
                    case_row[col] = returnT['T=0'] if order_fulfilment else ''
                # Treturn1=('c(T+1)'/'O(T)'-1)*100+'%' if order_fulfilment else ''
                elif col == 'T=1 Return':
                    case_row[col] = returnT['T=1'] if order_fulfilment else ''
                # buyingprice1=1.002*O(T)
                elif col == 'Buying price':
                    buying_price=calculate(history['T=0']['Open'],1.002,'*')
                    case_row[col] = buying_price
                # Treturn01=(c(T)/buyingprice1-1   )*100%
                elif col == 'T=0 Return':
                    case_row[col] = str(calculate(calculate(calculate(history['T=0']['Close'],buying_price,'/'),1,'-'),100,'*'))+'%'
                # Treturn11=(c(T+1)/buyingprice1-1   )*100%
                elif col == 'T=1 Return(1)':#
                    case_row[col] = str(calculate(calculate(calculate(history['T+1']['Close'],buying_price,'/'),1,'-'),100,'*'))+'%'
                # buyingprice2=buyingprice1 if O(T)<C(T_1) else ''
                elif col == 'Buying price':
                    case_row[col] = buying_price if convert2float(history['T=0']['Open']) < convert2float(history['T=0']['Close']) else ''
                # Treturn02=Treturn01 if buyingprice2 != '' else=''
                elif col == 'T=0 Return(2)':
                    case_row[col] = case_row['T=0 Return(1)'] if buying_price1!='' else ''
                # Treturn12=Treturn11 if buyingprice2 != '' else=''
                elif col == 'T=1 Return(2)':
                    case_row[col] = case_row['T=1 Return(1)'] if buying_price1!='' else ''
        #print('case row',case_row)
        if case['Negative Price drift']:
            NPDFrame.loc[case_index1]=case_row
            case_index1 += 1
            print('case1 index',case_index1)
            print('frame')
            print(NPDFrame)


        if case['Negative T-1 price change']:
            NTPCFrame.loc[case_index2]=case_row
            case_index2 += 1
            print('case2 index',case_index2)
            print('frames should be here')
            print(NTPCFrame)
    conditions = ['Positive EPS Surprise', 'Positive Rev. surprise','Negative Price drift','Positive Price drift','Negative T-1 price change','Positive T-1 price change']
    conditions_frame = pd.DataFrame(columns=['Conditions','T=0','T+1','T+2','T+3','T+4','T+5','Number of events'])
    i=0
    for c in conditions:
        sum,no_events,return_per_day=summ(c)
        row=[c,sum[0],sum[1],sum[2],sum[3],sum[4],sum[5],no_events]
        row2 = ['Return per day per event', calculate(sum[0],no_events,'/'), calculate(sum[1],no_events,'/'), calculate(sum[2],no_events,'/'),
                calculate(sum[3],no_events,'/'), calculate(sum[4],no_events,'/'), calculate(sum[5],no_events,'/'), '']
        conditions_frame.loc[i]=row
        i+=1
        conditions_frame.loc[i] = row2
        i += 1
    def highlight_max(x, color):
        return np.where(x >0.60, f"color: {color};", None)

    conditions_frame.style.apply(highlight_max, color='green')
    file_name = 'stocks data/'+company+'.xlsx'
    with pd.ExcelWriter(file_name, mode='w') as writer:
        main_frame.to_excel(writer, sheet_name='Sheet1')
        conditions_frame.to_excel(writer,sheet_name='Sheet1',startrow=len(main_frame)+5,startcol=5)
        df=pd.DataFrame(columns=['Name'])
        df['Name']=['Negative Price drift']
        df.to_excel(writer,sheet_name='Sheet2')
        NPDFrame.to_excel(writer, sheet_name='Sheet2',startrow=1,startcol=1)
        df['Name'] = ['Negative T-1 price change']
        df.to_excel(writer, sheet_name='Sheet2', startrow=len(NPDFrame['Time']) + 5)
        NTPCFrame.to_excel(writer,sheet_name='Sheet2',startrow=len(NPDFrame['Time'])+4,startcol=1)

    #Create a ZipFile Object
    with ZipFile('stocks_data.zip', 'w') as zip_object:
        zip_object.write(file_name)
        # os.remove(file_name)
    finished_list.append(symbol)
    finished_frame=pd.DataFrame(columns=['Symbol'])
    finished_frame['Symbol']=finished_list
    with pd.ExcelWriter('finished.xlsx', mode='w') as writer:
        finished_frame.to_excel(writer, sheet_name='Sheet1')
# except Exception as e:
#     print(symbol)
#     print(e)
#
#     err_msg=err_msg+'\n'+str(e)
#     with open('errors/error_'+company+'.log','w') as f:
#       f.write(err_msg)
#     str(sys.exc_info()[-1].tb_lineno)
