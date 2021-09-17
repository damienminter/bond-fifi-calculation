# Sample data
cashflows = {'2021-01-04':765, '2021-01-15':-540, '2021-01-22':-252.50}
AS_OF_DATE = '2021-01-25'
DAILY_EFFECTIVE_INTEREST_RATE = 0.000170

# Function to get days difference between 2 days
def days_diff_between_dates(start_date, finish_date):
    
    reporting_date = pd.to_datetime(start_date)
    trade_date = pd.to_datetime(finish_date)
    days_diff = (reporting_date - trade_date).days
    
    return days_diff



# Function to get opening bal, closing bal, acc effective interest etc
def cumulative_effective_interest_to_date(cob, cashflows, daily_eir):
    
    # Get date of first cashflow in dict 
    start_date = next(iter(cashflows))

    # Add the cob to list of dates and sort in date order
    cashflows.update({cob: 0})
    cashflows = dict(sorted(cashflows.items()))

    # print(cashflows)
    closing_bal = 0

    items = []

    
    
    for date, amount in cashflows.items():
        # while date <= cob:
            
        # Opening Balance
        opening_bal = closing_bal
        # Calc cumulative eir for period to each date
        days_in_period = days_diff_between_dates(date, start_date)
        period_eir = days_in_period * daily_eir
        # Calc cumulative cashflows for each date
        effective_interest = opening_bal * period_eir
        # Cashflows
        # amount
        # Closing Balance
        closing_bal = opening_bal + effective_interest + amount
        row = [date, days_in_period, opening_bal, daily_eir, period_eir, effective_interest, amount, closing_bal]
        items.append(row) 
        # Set startdate to last date
        start_date = date

    df = pd.DataFrame(items, columns=['date', 'days_in_period', 'opening_bal', 'daily_eir', 'period_eir', 'effective_interest', 'amount', 'closing_bal'])
   

    return df


    # period_eir += period_eir
    # amount += amount
    # print(period_eir, amount)
    
        
# issue with more than 1 thing happening on a day - ie coupon and sale
cumulative_effective_interest_to_date(AS_OF_DATE, cashflows, DAILY_EFFECTIVE_INTEREST_RATE)
