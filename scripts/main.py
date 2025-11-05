import psycopg2, pandas as pd, matplotlib.pyplot as plt, plotly.express as px
from openpyxl.formatting.rule import ColorScaleRule
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import config

def export_excel(dataframes, filename):
    filepath = f"exports/{filename}"
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            ws.freeze_panes = "B2"
            ws.auto_filter.ref = ws.dimensions
            for col in ws.iter_cols(min_row=2):
                if any(isinstance(cell.value, (int, float)) for cell in col[1:] if cell.value):
                    rule = ColorScaleRule(start_type="min", start_color="FFAA0000",
                                         mid_type="percentile", mid_value=50, mid_color="FFFFFF00",
                                         end_type="max", end_color="FF00AA00")
                    ws.conditional_formatting.add(f"{col[0].column_letter}2:{col[0].column_letter}{len(df)+1}", rule)
    print(f"Created {filename}, {len(dataframes)} sheets, {sum(len(df) for df in dataframes.values())} rows")
    return filepath

def create_chart(df, chart_type, business_question):
    plt.style.use('default')
    plt.rcParams['figure.figsize'] = (12, 8)
    plt.rcParams['font.size'] = 10
    chart_path = f"charts/{chart_type}_{business_question.replace(' ', '_')}.png"
    
    if chart_type == 'pie' and len(df.columns) >= 2:
        plt.pie(df.iloc[:, 1] if df.iloc[:, 1].dtype in ['int64', 'float64'] else range(len(df)),
                labels=df.iloc[:, 0].astype(str), autopct='%1.1f%%', startangle=90)
    elif chart_type == 'bar' and len(df.columns) >= 2:
        plt.bar(df.iloc[:, 0].astype(str), df.iloc[:, 1] if df.iloc[:, 1].dtype in ['int64', 'float64'] else range(len(df)))
        plt.xticks(rotation=45)
        plt.xlabel(df.columns[0])
        plt.ylabel(df.columns[1])
    elif chart_type == 'barh' and len(df.columns) >= 2:
        plt.barh(df.iloc[:, 0].astype(str), df.iloc[:, 1] if df.iloc[:, 1].dtype in ['int64', 'float64'] else range(len(df)))
        plt.xlabel(df.columns[1])
        plt.ylabel(df.columns[0])
    elif chart_type == 'line' and len(df.columns) >= 2:
        text_cols = [c for c in df.columns if df[c].dtype not in ['int64', 'float64']]
        num_cols = [c for c in df.columns if df[c].dtype in ['int64', 'float64']]
        x_col, y_col = (text_cols[0] if text_cols else df.columns[0], num_cols[0] if num_cols else df.columns[1])
        df_plot = df[[x_col, y_col]].copy()
        df_plot[y_col] = pd.to_numeric(df_plot[y_col], errors='coerce')
        df_plot = df_plot.dropna()
        if len(df_plot) > 0:
            plt.plot(df_plot[x_col].astype(str), df_plot[y_col], marker='o')
            plt.xticks(rotation=45)
            plt.xlabel(x_col)
            plt.ylabel(y_col)
    elif chart_type == 'hist':
        for col in df.columns:
            series = pd.to_numeric(df[col], errors='coerce')
            if series.dtype in ['int64', 'float64'] and not series.isna().all():
                data = series.dropna()
                if len(data) > 0:
                    plt.hist(data, bins=min(20, max(5, len(data)//4)), alpha=0.75, edgecolor='black')
                    plt.xlabel(col)
                    plt.ylabel('Frequency')
                    plt.grid(True, alpha=0.3)
                    break
    elif chart_type == 'scatter' and len(df.columns) >= 2:
        text_cols = [c for c in df.columns if df[c].dtype not in ['int64', 'float64']]
        num_cols = [c for c in df.columns if df[c].dtype in ['int64', 'float64']]
        x_col, y_col = (text_cols[0] if text_cols else df.columns[0], num_cols[0] if num_cols else df.columns[1])
        df_plot = df[[x_col, y_col]].copy()
        df_plot[y_col] = pd.to_numeric(df_plot[y_col], errors='coerce')
        df_plot = df_plot.dropna()
        if len(df_plot) > 0:
            plt.scatter(df_plot[x_col].astype(str), df_plot[y_col], alpha=0.6)
            plt.xticks(rotation=45)
            plt.xlabel(x_col)
            plt.ylabel(y_col)
    
    plt.title(f'{chart_type.title()} Chart: {business_question}')
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Created {chart_type} chart: {business_question} ({len(df)} rows) -> {chart_path}")
    return chart_path

def create_interactive_chart(df, business_question):
    year_cols = [c for c in df.columns if c.startswith('Production_')]
    if year_cols:
        df_long = pd.melt(df, id_vars=['Country'], value_vars=year_cols, var_name='Year', value_name='Production')
        df_long['Year'] = df_long['Year'].str.replace('Production_', '').astype(int)
        df_long['Production'] = pd.to_numeric(df_long['Production'], errors='coerce')
        df_long = df_long.dropna()
        if len(df_long) > 0:
            fig = px.scatter(df_long, x='Country', y='Production', animation_frame='Year', size='Production',
                           hover_data=['Country', 'Year', 'Production'], title=f'Interactive: {business_question}',
                           labels={'Country': 'Country', 'Production': 'Uranium Production (tonnes)'})
            fig.update_layout(title_x=0.5, width=1200, height=700, xaxis={'tickangle': 45})
            fig.show()
            print(f"Interactive chart: {business_question} ({len(df_long)} points, {df_long['Year'].min()}-{df_long['Year'].max()})")
            return fig
    return None

def run_queries():
    queries = [
        {'name': '1. Nuclear power plants by country', 'query': '''
            SELECT country_long as "Country", COUNT(*) as "NPP Count" FROM powerplants 
            WHERE primary_fuel = 'Nuclear' GROUP BY country_long ORDER BY COUNT(*) DESC LIMIT 10
        ''', 'chart_type': 'pie', 'business_question': 'Distribution of nuclear power plants by country'},
        {'name': '2. Top-10 countries by uranium production', 'query': '''
            SELECT country_long as "Country", SUM(production_tonnes_uranium) as "Uranium_Production_Tonnes" FROM mines 
            WHERE country_long IS NOT NULL GROUP BY country_long ORDER BY SUM(production_tonnes_uranium) DESC LIMIT 10
        ''', 'chart_type': 'bar', 'business_question': 'Top-10 countries by uranium production'},
        {'name': '3. Average nuclear plant capacity by country', 'query': '''
            SELECT country_long as "Country", AVG(NULLIF(capacity_in_mw::numeric, 0)) as "Average Capacity of NPPs by MegaWatts" FROM powerplants 
            WHERE capacity_in_mw IS NOT NULL AND NULLIF(TRIM(primary_fuel), '') IS NOT NULL AND TRIM(primary_fuel) ILIKE '%nuclear%'
            GROUP BY country_long ORDER BY "Average Capacity of NPPs by MegaWatts" DESC NULLS LAST LIMIT 10
        ''', 'chart_type': 'barh', 'business_question': 'Average capacity of nuclear plants by country'},
        {'name': '4. Reserves and Production', 'query': '''
            SELECT r.country_long as "Country", r.tonnes_of_uranium as "Reserves_Tonnes",
                   COALESCE(m.production_tonnes_uranium, 0) as "Production_Tonnes" FROM reserves r
            LEFT JOIN mines m ON r.country_long = m.country_long ORDER BY r.tonnes_of_uranium DESC LIMIT 15
        ''', 'chart_type': 'scatter', 'business_question': 'Relationship between reserves and production by country'},
        {'name': '5. Distribution of Nuclear Plant Capacities', 'query': '''
            SELECT capacity_in_mw as "Capacity in MegaWatts" FROM powerplants 
            WHERE primary_fuel = 'Nuclear' AND capacity_in_mw IS NOT NULL AND capacity_in_mw > 0 ORDER BY capacity_in_mw
        ''', 'chart_type': 'hist', 'business_question': 'Distribution of Nuclear Plant Capacities'},
        {'name': '6. Uranium production top companies', 'query': '''
            SELECT c.company as "Company", c.tonnes_uranium as "Production_Tonnes" FROM companies c
            WHERE c.tonnes_uranium > 1000 ORDER BY c.tonnes_uranium DESC LIMIT 20
        ''', 'chart_type': 'line', 'business_question': 'Uranium production by top companies'}
    ]
    interactive_queries = [{
        'name': 'Interactive Chart: Uranium Production Over Time',
        'query': '''
            SELECT country_long as "Country", "2007" as "Production_2007", "2008" as "Production_2008", 
                   "2009" as "Production_2009", "2010" as "Production_2010", "2011" as "Production_2011",
                   "2012" as "Production_2012", "2013" as "Production_2013", "2014" as "Production_2014" FROM production
            WHERE country_long IS NOT NULL ORDER BY "2014" DESC NULLS LAST LIMIT 15
        ''', 'business_question': 'Uranium Production Trends by Country'
    }]
    excel_queries = [
        {'name': 'Nuclear power plants', 'query': '''
            SELECT p.name_of_powerplant as "Powerplant", p.country_long as "Country",
                   p.capacity_in_mw as "Capacity_MW", p.primary_fuel as "Primary_Fuel" FROM powerplants p
            WHERE p.primary_fuel = 'Nuclear' ORDER BY p.capacity_in_mw DESC
        '''},
        {'name': 'Uranium companies', 'query': '''
            SELECT c.company as "Company", c.tonnes_uranium as "Production_Tonnes" FROM companies c ORDER BY c.tonnes_uranium DESC
        '''},
        {'name': 'Uranium reserves', 'query': '''
            SELECT r.country_long as "Country", r.tonnes_of_uranium as "Reserves_Tonnes" FROM reserves r ORDER BY r.tonnes_of_uranium DESC
        '''}
    ]
    
    try:
        conn = psycopg2.connect(**config())
        cur = conn.cursor()
        print('Connected to PostgreSQL database...')
        print(cur.fetchone()[0] if cur.execute('select version()') else None)
        
        print("\n" + "="*100 + "\nCREATING VISUALIZATIONS\n" + "="*100)
        for q in queries:
            print(f"\n{q['name']}\n" + "-"*50)
            try:
                cur.execute(q['query'])
                rows, cols = cur.fetchall(), [desc[0] for desc in cur.description]
                if rows:
                    df = pd.DataFrame(rows, columns=cols)
                    create_chart(df, q['chart_type'], q['business_question'])
                    print(f"\nChart data:\n{df.to_string(index=False)}")
            except Exception as e:
                print(f"Error: {e}")
        
        print("\n" + "="*100 + "\nCREATING INTERACTIVE CHART\n" + "="*100)
        for q in interactive_queries:
            print(f"\n{q['name']}\n" + "-"*50)
            try:
                cur.execute(q['query'])
                rows, cols = cur.fetchall(), [desc[0] for desc in cur.description]
                if rows:
                    create_interactive_chart(pd.DataFrame(rows, columns=cols), q['business_question'])
            except Exception as e:
                print(f"Error: {e}")
        
        print("\n" + "="*100 + "\nEXPORT TO EXCEL\n" + "="*100)
        excel_data = {}
        for q in excel_queries:
            try:
                cur.execute(q['query'])
                rows, cols = cur.fetchall(), [desc[0] for desc in cur.description]
                if rows:
                    excel_data[q['name']] = pd.DataFrame(rows, columns=cols)
                    print(f"Prepared {q['name']} ({len(excel_data[q['name']])} rows)")
            except Exception as e:
                print(f"Error: {e}")
        
        if excel_data:
            try:
                print(f"Excel file created: {export_excel(excel_data, 'uranium_industry_report.xlsx')}")
            except Exception as e:
                print(f"Error: {e}")
        
        cur.close()
        conn.close()
        print('Database connection closed.')
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    run_queries()