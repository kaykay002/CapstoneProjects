import streamlit as st
from streamlit_option_menu import option_menu
import psycopg2
import pandas as pd 
import plotly.express as px
import requests
import json
#--------------------------------------------------------------------------------------------------
#------------------------DATA FRAMES--------------------------------------------------------------

#DataFrame Creation

#SQL connection:

mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password=12345,
                      database="Phonepe_database",
                      port=5432)

cursor=mydb.cursor()


#1.Aggregate Insurance df-------------------------------------->>>>>>

cursor.execute("Select * from aggregate_insurance")
mydb.commit()
T1= cursor.fetchall()
Aggregated_Insurance=pd.DataFrame(T1,columns=("State", "Year","Quarter","Transaction_Type", "Transaction_Count","Transaction_Amount"))

#2.Aggregate Transaction df----------------------------------->>>>>>>>

cursor.execute("Select * from aggregate_transaction")
mydb.commit()
T2= cursor.fetchall()
Aggregated_Transaction=pd.DataFrame(T2,columns=("State", "Year","Quarter","Transaction_Type", "Transaction_Count","Transaction_Amount"))

#3.Aggregate User df-------------------------------------------->>>>>>>>>

cursor.execute("Select * from aggregate_user")
mydb.commit()
T3= cursor.fetchall()
Aggregated_User=pd.DataFrame(T3,columns=("State", "Year","Quarter","Brand", "Transaction_Count","Percentage"))


#4.Map Insurance df-------------------------------------------------->>>>

cursor.execute("Select * from map_transaction")
mydb.commit()
T4= cursor.fetchall()
Map_Insurance=pd.DataFrame(T4,columns=("State", "Year","Quarter","District", "Transaction_Count","Transaction_Amount"))


#5.Map Transaction df-------------------------------------------------->>>>

cursor.execute("Select * from map_transaction")
mydb.commit()
T5= cursor.fetchall()
Map_Transaction=pd.DataFrame(T5,columns=("State", "Year","Quarter","District", "Transaction_Count","Transaction_Amount"))

#6.Map User df-------------------------------------------------->>>>

cursor.execute("Select * from map_user")
mydb.commit()
T6= cursor.fetchall()
Map_User=pd.DataFrame(T6,columns=("State", "Year","Quarter","District", "Registered_Users","App_Opens"))

#7.Top Insurance df-------------------------------------------------->>>>

cursor.execute("Select * from top_insurance")
mydb.commit()
T7= cursor.fetchall()
Top_Insurance=pd.DataFrame(T7,columns=("State", "Year","Quarter","Pincode", "Transaction_Count","Transaction_Amount"))

#8.Top Transaction df-------------------------------------------------->>>>

cursor.execute("Select * from top_transaction")
mydb.commit()
T8= cursor.fetchall()
Top_Transaction=pd.DataFrame(T8,columns=("State", "Year","Quarter","Pincode", "Transaction_Count","Transaction_Amount"))

#9.Top Users df-------------------------------------------------->>>>

cursor.execute("Select * from top_user")
mydb.commit()
T9= cursor.fetchall()
Top_User=pd.DataFrame(T9,columns=("State", "Year","Quarter","Pincode", "Registered_Users"))
#-------------------------------------------------------------------------------------------------

def transactions_yearwise(df,year):
    transy=df[df["Year"]==year]
    transy.reset_index(drop=True, inplace=True)

    transyg=transy.groupby("State")[["Transaction_Count", "Transaction_Amount"]].sum()
    transyg.reset_index(inplace=True)

    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url)
    data_map=json.loads(response.content)
    state_name=[]
    for feature in data_map["features"]:
        state_name.append(feature["properties"]["ST_NM"])
    state_name.sort()

#Transaction_amount
    amount_fig=px.bar(transyg, x="State", y="Transaction_Amount",title=f"Transaction Amount of {year}", color_discrete_sequence=px.colors.diverging.Spectral,height=570, width=650)
    
       
    india_map1=px.choropleth(transyg, geojson=data_map, locations="State", featureidkey="properties.ST_NM",
                            color="Transaction_Amount", color_continuous_scale="thermal",
                            range_color=(transyg["Transaction_Amount"].min(),transyg["Transaction_Amount"].max()),
                            hover_name="State", fitbounds="locations",
                            height=600, width=600 )
    india_map1.update_geos(visible=False)

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(amount_fig)     
    with col2:
        st.plotly_chart(india_map1)

#Transaction_count
    count_fig=px.bar(transyg, x="State", y="Transaction_Count",title=f"Transaction Count of {year}",color_discrete_sequence=px.colors.qualitative.Prism, height=570, width=650)
    
    india_map2=px.choropleth(transyg, geojson=data_map, locations="State", featureidkey="properties.ST_NM",
                            color="Transaction_Count", color_continuous_scale="thermal",
                            range_color=(transyg["Transaction_Count"].min(),transyg["Transaction_Count"].max()),
                            hover_name="State", fitbounds="locations",
                            height=600, width=600 )
    india_map2.update_geos(visible=False)

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(count_fig)
    with col2:
        st.plotly_chart(india_map2)

    return transy
#------------------------------------------------------------------------------------------------
def transactions_yearwise_quarter(df,quarter):
    transy=df[df["Quarter"]==quarter]
    transy.reset_index(drop=True, inplace=True)

    transyg=transy.groupby("State")[["Transaction_Count", "Transaction_Amount"]].sum()
    transyg.reset_index(inplace=True)

    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url)
    data_map=json.loads(response.content)
    state_name=[]
    for feature in data_map["features"]:
        state_name.append(feature["properties"]["ST_NM"])
    state_name.sort()

#Transaction_amount
    amount_fig=px.bar(transyg, x="State", y="Transaction_Amount",title=f"Transaction Amount of Year {transy['Year'].min()} : Quarter {quarter}", color_discrete_sequence=px.colors.diverging.Spectral,height=400, width=550)
    
       
    india_map1=px.choropleth(transyg, geojson=data_map, locations="State", featureidkey="properties.ST_NM",
                            color="Transaction_Amount", color_continuous_scale="thermal",
                            range_color=(transyg["Transaction_Amount"].min(),transyg["Transaction_Amount"].max()),
                            hover_name="State", title=f"Transaction Amount of Year {transy['Year'].min()} : Quarter {quarter}", fitbounds="locations",
                            height=600, width=650 )
    india_map1.update_geos(visible=False)

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(amount_fig)
    with col2:
        st.plotly_chart(india_map1)

#Transaction_count
    count_fig=px.bar(transyg, x="State", y="Transaction_Count",title=f"Transaction Count of Year {transy['Year'].min()} : Quarter {quarter}",color_discrete_sequence=px.colors.qualitative.Prism, height=400, width=650)
    
    india_map2=px.choropleth(transyg, geojson=data_map, locations="State", featureidkey="properties.ST_NM",
                            color="Transaction_Count", color_continuous_scale="thermal",
                            range_color=(transyg["Transaction_Count"].min(),transyg["Transaction_Count"].max()),
                            hover_name="State", title=f"Transaction Count of Year {transy['Year'].min()} : Quarter {quarter}", fitbounds="locations",
                            height=600, width=650 )
    india_map2.update_geos(visible=False)

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(count_fig)
    with col2:
        st.plotly_chart(india_map2)
#-----------------------------------------------------------------------------------------------
def Agg_Analysis_Transaction(df, state):

    transy=df[df["State"]==state]
    transy.reset_index(drop=True, inplace=True)

    transyg=transy.groupby("Transaction_Type")[["Transaction_Count", "Transaction_Amount"]].sum()
    transyg.reset_index(inplace=True)

    col1,col2=st.columns(2)
    
    fig_pie1=px.pie(data_frame=transyg, names="Transaction_Type", values="Transaction_Amount",
                    width=500, title=f"{state} Transaction Amount", hole=0.4, color_discrete_sequence=px.colors.qualitative.Prism)
    with col1:
        st.plotly_chart(fig_pie1)

    fig_pie2=px.pie(data_frame=transyg, names="Transaction_Type", values="Transaction_Count",
                    width=500, title=f"{state} Transaction Count", hole=0.4, color_discrete_sequence=px.colors.qualitative.Prism)

    
    with col2:
        st.plotly_chart(fig_pie2) 

  
#----------------Streamlit User Interface-----------------#

st.set_page_config(layout="wide")
col1, col2 = st.columns(2)

with col1:
    st.image("1.png", width=100)

with col2:
    st.title("PhonePe Pulse Dashboard")
    


#----------------------------------------MAIN MENU------------------------------------------------------
SELECT = option_menu(
    menu_title = None,
    options = ["About","Maps & Charts","FAQs"],
    icons =["phone","bar-chart","book"],
    default_index=2,
    orientation="horizontal",
    styles={"container": {"padding": "0!important", "background-color": "white","size":"medium", "width": "100%"},
        "icon": {"color": "white", "font-size": "15px"},
        "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px", "--hover-color": "none"},
        "nav-link-selected": {"background-color": "#4E049C"}})
#----------------------------------------------ABOUT-------------------------------------------------------
if SELECT == "About":
    st.write('''The Phonepe Pulse Dashboard is a dynamic visualization platform meticulously crafted to 
            delve into the extensive data reservoir of the Phonepe Pulse Github repository. Boasting 
            seamless data extraction, processing, and visualization capabilities, this dashboard offers 
            an immersive experience, unveiling insights and trends through interactive geo-visualizations. 
            With a user-friendly interface enriched by dropdown options, users can effortlessly tailor 
            their analysis to extract meaningful insights. Powered by continuous updates and efficient 
            MySQL database integration, the dashboard ensures users are equipped with the latest information 
            for informed decision-making. Experience the fusion of innovation and usability, driving data-driven 
            excellence with the Phonepe Pulse Dashboard.''')


#-----------------------------------------ANALYSIS---------------------------------------------------------
if SELECT == "Maps & Charts":
    with st.sidebar:
        SELECT = st.selectbox("Select Analysis of", ["Aggregated Analysis", "Map Analysis", "Top Analysis"])
        
        if SELECT == "Aggregated Analysis":
            method=st.radio("Select the Aggregated Analysis category, By:-",["Insurance","Transaction","User"])

if method=="Insurance":
    year = st.slider("Select the year", Aggregated_Insurance["Year"].min(), Aggregated_Insurance["Year"].max(), Aggregated_Insurance["Year"].min())
    transyear=transactions_yearwise(Aggregated_Insurance, year)

    with st.sidebar:   
        quarter = st.slider("Select the quarter", transyear["Quarter"].min(), transyear["Quarter"].max(), transyear["Quarter"].min())
    quarter=transactions_yearwise_quarter(transyear,quarter)

elif method=="Transaction":
    year = st.slider("Select the year", Aggregated_Transaction["Year"].min(), Aggregated_Transaction["Year"].max(), Aggregated_Transaction["Year"].min())
    Agg_trans_yearly=transactions_yearwise(Aggregated_Transaction, year)

    states=st.selectbox("Select State", Agg_trans_yearly["State"].unique())
    Agg_Analysis_Transaction(Agg_trans_yearly, states)


    

            
           
            
        



'''elif SELECT == "Map Analysis":
    
    method2=st.radio("Select the Map Analysis category, By:-",["Insurance","Transaction","User"])
    
    if method2=="Insurance":
        pass
    elif method2=="Transaction":
        pass
    elif method2=="User":
        pass

elif SELECT == "Top Analysis":
    
    method3=st.radio("Select the Top Analysis category, By:-",["Insurance","Transaction","User"])

    if method3=="Insurance":
        pass
    elif method3=="Transaction":
        pass
    elif method3=="User":
        pass'''

#---------------------------------------MAPS AND CHARTS------------------------------------------------   
if SELECT == "Maps & Charts":
    with st.sidebar:
        SELECT = st.selectbox("Select Map", ["Map1", "Map2", "Map3"])

if SELECT == "Map1":
    st.write("Content for Map1 goes here.")
    # Add content related to Map1

elif SELECT == "Map2":
    st.write("Content for Map2 goes here.")
    # Add content related to Map2

elif SELECT == "Map3":
    st.write("Content for Map3 goes here.")
    # Add content related to Map3





















#---------------------------------------------FAQS----------------------------------------------------------
if SELECT == "FAQs":
    options = ["SELECT",
               "Top 10 states based on year and amount of transaction",
               "List 10 states based on type and amount of transaction",
               "Top 5 Transaction_Type based on Transaction_Amount",
               "Top 10 Registered-users based on States and District",
               "Top 10 Districts based on states and Count of transaction",
               "List 10 Districts based on states and amount of transaction",
               "List 10 Transaction_Count based on Districts and states",
               "Top 10 RegisteredUsers based on states and District"]

    #1
               
    select = st.selectbox("Select the option",options)
    if select=="Top 10 states based on year and amount of transaction":
        cursor.execute("SELECT DISTINCT States, Transaction_Year, SUM(Transaction_Amount) AS Total_Transaction_Amount FROM top_tran GROUP BY States, Transaction_Year ORDER BY Total_Transaction_Amount DESC LIMIT 10");
        
        df = pd.DataFrame(cursor.fetchall(), columns=['States','Transaction_Year', 'Transaction_Amount'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 states and amount of transaction")
            st.bar_chart(data=df,x="Transaction_Amount",y="States")
            
            #2
            
    elif select=="List 10 states based on type and amount of transaction":
        cursor.execute("SELECT DISTINCT States, SUM(Transaction_Count) as Total FROM top_tran GROUP BY States ORDER BY Total ASC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['States','Total_Transaction'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("List 10 states based on type and amount of transaction")
            st.bar_chart(data=df,x="Total_Transaction",y="States")
            
            #3
            
    elif select == "Top 5 Transaction_Type based on Transaction_Amount":
        cursor.execute("SELECT DISTINCT Transaction_Type, SUM(Transaction_Amount) AS Amount FROM agg_user GROUP BY Transaction_Type ORDER BY Amount DESC LIMIT 5")
        df = pd.DataFrame(cursor.fetchall(), columns=['Transaction_Type', 'Transaction_Amount'])
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 5 Transaction_Type based on Transaction_Amount")
            st.bar_chart(data=df, y="Transaction_Type", x="Transaction_Amount")

            #4
            
    elif select=="Top 10 Registered-users based on States and District":
        cursor.execute("SELECT DISTINCT State, District, SUM(RegisteredUsers) AS Users FROM top_user GROUP BY State, District ORDER BY Users DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['State','District','RegisteredUsers'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Registered-users based on States and District")
            st.bar_chart(data=df,y="State",x="RegisteredUsers")
            
            #5
            
    elif select=="Top 10 Districts based on states and Count of transaction":
        cursor.execute("SELECT DISTINCT States,District,SUM(Transaction_Count) AS Counts FROM map_tran GROUP BY States,District ORDER BY Counts DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['States','District','Transaction_Count'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Districts based on states and Count of transaction")
            st.bar_chart(data=df,y="States",x="Transaction_Count")
            
            #6
            
    elif select=="List 10 Districts based on states and amount of transaction":
        cursor.execute("SELECT DISTINCT States,Transaction_year,SUM(Transaction_Amount) AS Amount FROM agg_trans GROUP BY States, Transaction_year ORDER BY Amount ASC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['States','Transaction_year','Transaction_Amount'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 Districts based on states and amount of transaction")
            st.bar_chart(data=df,y="States",x="Transaction_Amount")
            
            #7
            
    elif select=="List 10 Transaction_Count based on Districts and states":
        cursor.execute("SELECT DISTINCT States, District, SUM(Transaction_Count) AS Counts FROM map_tran GROUP BY States,District ORDER BY Counts ASC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['States','District','Transaction_Count'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("List 10 Transaction_Count based on Districts and states")
            st.bar_chart(data=df,y="States",x="Transaction_Count")
            
            #8
             
    elif select=="Top 10 RegisteredUsers based on states and District":
        cursor.execute("SELECT DISTINCT States,District, SUM(RegisteredUsers) AS Users FROM map_user GROUP BY States,District ORDER BY Users DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns = ['States','District','RegisteredUsers'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 RegisteredUsers based on states and District")
            st.bar_chart(data=df,y="States",x="RegisteredUsers")

 
