#!/usr/bin/env python
# coding: utf-8

# In[29]:


import pandas as pd
import glob
from sqlalchemy import create_engine, text
from datetime import datetime


# In[30]:


# 1. Congfig Koneksi DB
DB_HOST = 'localhost' 
DB_PORT = '3307' 
DB_USER = 'root'
DB_PASS = 'adminpassword'
DB_NAME = 'maju_jaya'

#url connection
connection_url = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(
    connection_url,
    connect_args={
        'auth_plugin': 'mysql_native_password'
    }
)


# In[31]:


#=====================================================
# TASK 1: INGEST TO DB (BRONZE LAYER)
#=====================================================

def task_1_ingest_csv():
    print("\n--- Task 1: Ingest CSV Files to DB ---")
    data_path = './data/customer_addresses_*.csv'
    files = glob.glob(data_path)

    if not files:
        print("Tidak ada file CSV ditemukan di folder data/")
        return

    for file in files:
        df = pd.read_csv(file, sep=';', engine='python', encoding='utf-8')

        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])

        #Ingest to MySQL
        df.to_sql('customer_addresses_raw', con=engine, if_exists='append', index=False)
        print(f"File {file} berhasil di-ingest ke table customer_addresses_raw")


# In[32]:


#=====================================================
# TASK 2.1: TRANSFORM DATA (SILVER LAYER)
#=====================================================

def transform_data():
    print("\n--- Task 2: Cleaning ---")

    with engine.begin() as conn:
        # Cleaning logic (Silver layer)
        print("Cleaning data...")

        #dim_customers
        conn.execute(text("""
        create or replace view v_dim_customers as 
        select
        	id,
        	name,
        	case
        		when dob regexp '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' then str_to_date(dob, '%Y/%m/%d')
        		when dob like '%/%/%' then str_to_date(dob, '%d/%m/%Y')
        		else cast(dob as date)
        	end dob,
        	created_at
        from customers_raw
        where dob != '1900-01-01' or dob is null;
        """))

        #dim_customer_address
        conn.execute(text("""
        create or replace view v_dim_customer_address as
        select
        	id,
        	customer_id,
        	address,
        	case
        		when city like '%Pusat%' then 'Jakarta Pusat'
        		else city
        	end as city,
        	province,
        	created_at
        from customer_addresses_raw
        """))

        #fact_service_logs
        conn.execute(text("""
        create or replace view v_fact_service_logs as 
        select
        	service_ticket,
        	vin,
        	customer_id,
        	model,
        	service_date,
        	service_type,
        	created_at
        from after_sales_raw;
        """))

        #fact_sales
        conn.execute(text("""
        create or replace view v_fact_sales as
        select 
            vin,
            customer_id,
            model,
            cast(invoice_date as date) as invoice_date,
            cast(replace(price, '.', '') as decimal(15,2)) as price_numeric,
            created_at
        from sales_raw;
        """))


# In[33]:


#=====================================================
# TASK 2.2: DATAMART (GOLD LAYER)
#=====================================================

def view_datamart():
    print("\n--- Task 2.2: Creating Datamart")

    with engine.begin() as conn:
        #datamart (Gold Layer)
        print("Creating Datamart Tables...")

        #Menyiapkan skema tabel
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dm_sales_performance (
            periode VARCHAR(7),
            class VARCHAR(10),
            model VARCHAR(100),
            total DECIMAL(18,2),
            updated_date TIMESTAMP
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dm_customer_loyalty (
            periode VARCHAR(4),
            vin VARCHAR(50),
            customer_name VARCHAR(200),
            address TEXT,
            count_service INT,
            priority VARCHAR(10),
            updated_date TIMESTAMP
        );
        """))

        # Report 1: Sales Performance
        conn.execute(text("""
        create or replace view v_dm_sales_performance as
        select 
            date_format(invoice_date, '%Y-%m') as periode,
            case 
                when price_numeric between 100000000 and 250000000 then 'LOW'
                when price_numeric > 250000000 and price_numeric <= 400000000 then 'MEDIUM'
                when price_numeric > 400000000 then 'HIGH'
            end as class,
            model,
            sum(price_numeric) as total,
            CURRENT_TIMESTAMP() as updated_date
        from v_fact_sales
        group by 1, 2, 3
        order by periode desc, total desc;
        """))

        # Report 2: Customer Loyalty
        conn.execute(text("""
        create or replace view v_dm_customer_loyalty as
        with service_count as (
            select 
                customer_id, 
                vin, 
                count(service_ticket) as count_service
            from after_sales_raw
            group by 1, 2
        )
        select 
            date_format(s.invoice_date, '%Y') as periode,
            s.vin,
            c.name as customer_name,
            addr.address,
            sc.count_service,
            case 
                when sc.count_service > 10 then 'HIGH'
                when sc.count_service between 5 and 10 then 'MED'
                else 'LOW'
            end as priority,
            CURRENT_TIMESTAMP() as updated_date
        from v_fact_sales s
        join v_dim_customers c on s.customer_id = c.id
        left join v_dim_customer_address addr on c.id = addr.customer_id
        left join service_count sc on s.vin = sc.vin;
        """))



# In[34]:


#=====================================================
# TASK 2.3: INGEST TO DATAMART USING INCREMENTAL PROCESSING
#=====================================================

def ingest_datamart():
    print("\n--- Task 2.3: Ingest Datamart")

    with engine.begin() as conn:
        #datamart (Gold Layer)
        print("Ingest Datamart Tables...")

        #Delete datamart
        conn.execute(text("""
        delete from dm_sales_performance
        where periode in (
        	select
        		periode
        	from (
        	select distinct
        		dm.periode
        	from dm_sales_performance dm
        	join v_dm_sales_performance vdm
        		on dm.periode = vdm.periode 
        	)x
        )
        """))

        conn.execute(text("""
        delete from dm_customer_loyalty 
        where periode in (
        	select
        		periode
        	from (
        	select distinct
        		dm.periode
        	from dm_customer_loyalty dm
        	join v_dm_customer_loyalty vdm
        		on dm.periode = vdm.periode 
        	)x
        )
        """))

        #Ingest datamart
        conn.execute(text("""
        insert into dm_sales_performance
        select * from v_dm_sales_performance
        """))

        conn.execute(text("""
        insert into dm_customer_loyalty
        select * from v_dm_customer_loyalty
        """))

        print("Datamart berhasil diperbarui!")

if __name__ == "__main__":
    task_1_ingest_csv()
    transform_data()
    view_datamart()
    ingest_datamart()


# In[ ]:




