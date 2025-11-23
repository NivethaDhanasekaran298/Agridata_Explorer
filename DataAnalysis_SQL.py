import mysql.connector
import pandas as pd

# ---------------------- DATABASE CONNECTION ----------------------
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="agri_data"
    )
    print("✅ Database connection successful!")
except mysql.connector.Error as e:
    print(f"❌ Database connection failed: {e}")
    exit()

# ---------------------- COLUMN HELPER ----------------------
def col(name):
    """Wrap column names in backticks for MySQL safety."""
    return f"`{name}`"

# ---------------------- SQL QUERIES ----------------------
queries = {
    "year-wise_total_rice_production_by_state.csv": f"""
        SELECT {col('YEAR')} AS Year, {col('STATE_NAME')} AS State_Name,
               SUM({col('RICE_PRODUCTION_(1000_TONS)')}) AS Total_Rice_Production
        FROM agriculture
        GROUP BY {col('YEAR')}, {col('STATE_NAME')}
        ORDER BY Year, Total_Rice_Production DESC;
    """,
    "top_5_districts_with_highest_wheat_yield_in_last_5_years.csv": f"""
        WITH wheat_change AS (
            SELECT {col('DIST_NAME')} AS District,
                   MAX({col('WHEAT_YIELD_(KG_PER_HA)')}) AS Max_Yield,
                   MIN({col('WHEAT_YIELD_(KG_PER_HA)')}) AS Min_Yield,
                   (MAX({col('WHEAT_YIELD_(KG_PER_HA)')}) - MIN({col('WHEAT_YIELD_(KG_PER_HA)')})) AS Yield_Increase
            FROM agriculture
            WHERE {col('YEAR')} >= (SELECT MAX({col('YEAR')})-5 FROM agriculture)
            GROUP BY {col('DIST_NAME')}
        )
        SELECT District, ROUND(Yield_Increase,2) AS Yield_Growth
        FROM wheat_change
        ORDER BY Yield_Increase DESC
        LIMIT 5;
    """,
    "top_5_states_with_highest_oilseed_production_growth_in_last_5_years.csv": f"""
        WITH oilseed_growth AS (
            SELECT {col('STATE_NAME')} AS State,
                   (MAX({col('OILSEEDS_PRODUCTION_(1000_TONS)')}) - MIN({col('OILSEEDS_PRODUCTION_(1000_TONS)')})) /
                   NULLIF(MIN({col('OILSEEDS_PRODUCTION_(1000_TONS)')}),0) * 100 AS Growth_Rate
            FROM agriculture
            WHERE {col('YEAR')} BETWEEN (SELECT MAX({col('YEAR')})-5 FROM agriculture) AND (SELECT MAX({col('YEAR')}) FROM agriculture)
            GROUP BY {col('STATE_NAME')}
        )
        SELECT State, ROUND(Growth_Rate,2) AS Percent_Growth
        FROM oilseed_growth
        ORDER BY Growth_Rate DESC
        LIMIT 5;
    """,
    "area_vs_production_summary_for_major_crops_by_district.csv": f"""
        SELECT {col('DIST_NAME')} AS District,
               SUM({col('RICE_AREA_(1000_HA)')}) AS Rice_Area,
               SUM({col('RICE_PRODUCTION_(1000_TONS)')}) AS Rice_Prod,
               SUM({col('WHEAT_AREA_(1000_HA)')}) AS Wheat_Area,
               SUM({col('WHEAT_PRODUCTION_(1000_TONS)')}) AS Wheat_Prod,
               SUM({col('MAIZE_AREA_(1000_HA)')}) AS Maize_Area,
               SUM({col('MAIZE_PRODUCTION_(1000_TONS)')}) AS Maize_Prod
        FROM agriculture
        GROUP BY {col('DIST_NAME')};
        
    """,
    "cotton_production_trend_for_top_5_cotton-producing_states.csv": f"""
        WITH top_states AS (
    SELECT `STATE_NAME` AS State
    FROM agriculture
    GROUP BY `STATE_NAME`
    ORDER BY SUM(`RICE_PRODUCTION_(1000_TONS)` + `WHEAT_PRODUCTION_(1000_TONS)`) DESC
    LIMIT 5
)
 SELECT 
    a.`YEAR` AS Year,
    a.`STATE_NAME` AS State,
    SUM(a.`RICE_PRODUCTION_(1000_TONS)`) AS Rice_Prod,
    SUM(a.`WHEAT_PRODUCTION_(1000_TONS)`) AS Wheat_Prod
FROM agriculture a
JOIN top_states t
      ON a.`STATE_NAME` = t.State
GROUP BY a.`YEAR`, a.`STATE_NAME`
ORDER BY Year, State;

    """,
    "top_10_districts_by_groundnut_production_in_2020.csv": f"""
        SELECT {col('DIST_NAME')} AS District, {col('STATE_NAME')} AS State,
               SUM({col('GROUNDNUT_PRODUCTION_(1000_TONS)')}) AS Total_Groundnut
        FROM agriculture
        WHERE {col('YEAR')} = 2020
        GROUP BY {col('DIST_NAME')}, {col('STATE_NAME')}
        ORDER BY Total_Groundnut DESC
        LIMIT 10;
    """,
    "year-wise_average_maize_yield.csv": f"""
        SELECT {col('YEAR')} AS Year, ROUND(AVG({col('MAIZE_YIELD_(KG_PER_HA)')}),2) AS Avg_Maize_Yield
        FROM agriculture  
        GROUP BY {col('YEAR')}
        ORDER BY {col('YEAR')};
    """,
    "state-wise_total_area_under_oilseeds.csv": f"""
        SELECT {col('STATE_NAME')} AS State, ROUND(SUM({col('OILSEEDS_AREA_(1000_HA)')}),2) AS Total_Area
        FROM agriculture
        GROUP BY {col('STATE_NAME')}
        ORDER BY Total_Area DESC;
    """,
    "top_10_districts_with_highest_average_rice_yield.csv": f"""
        SELECT {col('DIST_NAME')} AS District, {col('STATE_NAME')} AS State,
               ROUND(AVG({col('RICE_YIELD_(KG_PER_HA)')}),2) AS Avg_Rice_Yield
        FROM agriculture
        GROUP BY {col('DIST_NAME')}, {col('STATE_NAME')}
        ORDER BY Avg_Rice_Yield DESC
        LIMIT 10;
    """,
    "rice_vs_wheat_production_comparison_for_top_5_producing_states.csv": f"""
        WITH top_states AS (
            SELECT {col('STATE_NAME')} AS State
            FROM agriculture
            GROUP BY {col('STATE_NAME')}
            ORDER BY SUM({col('RICE_PRODUCTION_(1000_TONS)')} + {col('WHEAT_PRODUCTION_(1000_TONS)')}) DESC
            LIMIT 5
        )
        SELECT {col('YEAR')} AS Year, State,
               SUM({col('RICE_PRODUCTION_(1000_TONS)')}) AS Rice_Prod,
               SUM({col('WHEAT_PRODUCTION_(1000_TONS)')}) AS Wheat_Prod
        FROM agriculture
        WHERE {col('STATE_NAME')} IN (SELECT State FROM top_states)
        GROUP BY {col('YEAR')}, State
        ORDER BY {col('YEAR')}, State;
    """,
}

# ---------------------- EXECUTE QUERIES ----------------------
for file_name, query in queries.items():
    print(f"📌 Executing: {file_name}")
    try:
        df = pd.read_sql(query, conn)
        df.to_csv(file_name, index=False)
        print(f"✅ Saved: {file_name}")
    except Exception as e:
        print(f"❌ Error executing query for {file_name}: {e}")

# ---------------------- CLOSE CONNECTION ----------------------
conn.close()
print("✅ All SQL analyses complete! CSV files saved.")
