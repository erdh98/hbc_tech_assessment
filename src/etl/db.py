import pandas as pd
import duckdb
from pathlib import Path    
import yaml
import etl.utils as utils

class NYC311Database:

    def __init__(self, initialize: bool = True):
        self.data_path = str(Path.cwd().parent) + "/" + "data/311_requests/**/*.parquet"  
        self.table_name = "nyc311"
        if initialize:
            self.connection = duckdb.connect("nyc311.duckdb")
            self.connection.execute(f"""
                CREATE OR REPLACE TABLE {self.table_name} AS
                SELECT *
                FROM read_parquet('{self.data_path}', union_by_name=True)
            """)
        else:
            self.connection = duckdb.connect("nyc311_read.duckdb", read_only=True)
            ""

    def query_data_as_df(self, query: str):
        result = self.connection.execute(query).df()
        return result
    
    def refresh_data(self):
        self.connection.execute(f"""
            CREATE OR REPLACE TABLE {self.table_name} AS
            SELECT *
            FROM read_parquet('{self.data_path}', union_by_name=True)
        """)
    
    def create_table_from_df(self, df: pd.DataFrame, table_name: str):
        self.connection.register("temp_table", df)
        self.connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_table")
        self.connection.unregister("temp_table")   

    def join_to_base_table(self, table_name: str, join_column: str):
        self.connection.execute(f'''
            CREATE OR REPLACE TABLE {self.table_name} AS
            SELECT n.* REPLACE (t.* EXCLUDE ({join_column}) )
            FROM {self.table_name} n
            LEFT JOIN {table_name} t ON n.{join_column} = t.{join_column};
            ''')

    def alter_datatypes(self):
        self.connection.execute(f"""
            CREATE OR REPLACE TABLE {self.table_name} AS
            SELECT
                NULLIF(TRIM(unique_key), '')                         AS unique_key,
                NULLIF(TRIM(complaint_type), '')                     AS complaint_type,
                NULLIF(TRIM(descriptor), '')                         AS descriptor,
                UPPER(NULLIF(TRIM(borough), ''))                     AS borough,
                NULLIF(TRIM(incident_zip), '')                       AS incident_zip,
                NULLIF(TRIM(incident_address), '')                   AS incident_address,
                TRY_CAST(created_date AS TIMESTAMP)                  AS created_date,
                TRY_CAST(closed_date  AS TIMESTAMP)                  AS closed_date,
                TRY_CAST(due_date  AS TIMESTAMP)                     AS due_date,
                TRY_CAST(resolution_action_updated_date AS TIMESTAMP) AS resolution_action_updated_date,
                NULLIF(TRIM(status), '')                             AS status,
                NULLIF(TRIM(city), '')                               AS city,
                NULLIF(TRIM(street_name), '')                        AS street_name,   
                NULLIF(TRIM(cross_street_1), '')                     AS cross_street_1,
                NULLIF(TRIM(cross_street_2), '')                     AS cross_street_2,
                NULLIF(TRIM(intersection_street_1), '')              AS intersection_street_1,
                NULLIF(TRIM(intersection_street_2), '')              AS intersection_street_2,
                NULLIF(TRIM(address_type), '')                       AS address_type,
                NULLIF(TRIM(location_type), '')                      AS location_type,
                NULLIF(TRIM(agency), '')                             AS agency,
                NULLIF(TRIM(agency_name), '')                        AS agency_name,
                NULLIF(TRIM(park_facility_name), '')                 AS park_facility_name,
                NULLIF(TRIM(park_borough), '')                       AS park_borough,
                NULLIF(TRIM(vehicle_type), '')                       AS vehicle_type,
                NULLIF(TRIM(taxi_company_borough), '')               AS taxi_company_borough,
                NULLIF(TRIM(taxi_pick_up_location), '')              AS taxi_pick_up_location,
                NULLIF(TRIM(bridge_highway_name), '')                AS bridge_highway_name,
                NULLIF(TRIM(bridge_highway_direction), '')           AS bridge_highway_direction,
                NULLIF(TRIM(road_ramp), '')                          AS road_ramp,
                NULLIF(TRIM(bridge_highway_segment), '')             AS bridge_highway_segment,
                NULLIF(TRIM(community_board), '')                    AS community_board,
                NULLIF(TRIM(landmark), '')                           AS landmark,
                NULLIF(TRIM(resolution_description), '')             AS resolution_description,
                NULLIF(TRIM(bbl), '')                                AS bbl,
                NULLIF(TRIM(x_coordinate_state_plane), '')           AS x_coordinate_state_plane,
                NULLIF(TRIM(y_coordinate_state_plane), '')           AS y_coordinate_state_plane,
                NULLIF(TRIM(open_data_channel_type), '')             AS open_data_channel_type,
                NULLIF(TRIM(facility_type), '')                      AS facility_type,
                CAST(latitude AS DOUBLE)  AS latitude,
                CAST(longitude AS DOUBLE) AS longitude,
                location
            FROM {self.table_name};
        """)

    def remove_unique_key_duplicates(self):
        self.connection.execute(f"""
            CREATE TEMP TABLE duplicate_records AS
            SELECT unique_key
            FROM {self.table_name}
            GROUP BY unique_key
            HAVING COUNT(*) > 1;

            CREATE TEMP TABLE duplicates_removed AS
            SELECT * EXCLUDE (ranking)
            FROM (
            SELECT temp.*, ROW_NUMBER() OVER (
                PARTITION BY temp.unique_key
                ORDER BY temp.created_date DESC, temp.unique_key DESC
                ) AS ranking
            FROM {self.table_name} AS temp
            WHERE temp.unique_key IN (SELECT unique_key FROM duplicate_records)
            ) s
            WHERE ranking = 1;

            DELETE FROM {self.table_name}
            WHERE unique_key IN (SELECT unique_key FROM duplicate_records);

            INSERT INTO {self.table_name}
            SELECT * FROM duplicates_removed;

            DROP TABLE duplicate_records;
            DROP TABLE duplicates_removed;
        """)

    def standardize_zip_codes(self):
        self.connection.execute(f'''
            UPDATE {self.table_name}
            SET incident_zip = NULL
            WHERE incident_zip IS NOT NULL
            AND NOT REGEXP_MATCHES(incident_zip, '^[0-9]{{5}}(-[0-9]{{4}})?$'); ''')
    
    def update_values_to_null(self, column_name: str, invalid_values: tuple):
        self.connection.execute(f'''
            UPDATE {self.table_name}
            SET {column_name} = NULL
            WHERE {column_name} IN {invalid_values};''')
    
    def add_granular_datequery_columns(self):
        self.connection.execute(f'''
            ALTER TABLE {self.table_name}
            ADD COLUMN IF NOT EXISTS year INTEGER;
            ALTER TABLE {self.table_name}
            ADD COLUMN IF NOT EXISTS month INTEGER;
            ALTER TABLE {self.table_name}
            ADD COLUMN IF NOT EXISTS day INTEGER;
            ALTER TABLE {self.table_name}
            ADD COLUMN IF NOT EXISTS day_of_week INTEGER;

            UPDATE {self.table_name}
            SET year = EXTRACT(YEAR FROM created_date),
                month = EXTRACT(MONTH FROM created_date),
                day = EXTRACT(DAY FROM created_date),
                day_of_week = EXTRACT(DOW FROM created_date);''')
    

    def impute_missing_boroughs(self):
        self.connection.execute(f'''
            CREATE TEMP TABLE null_zips AS
            SELECT DISTINCT incident_zip
            FROM {self.table_name}
            WHERE borough IS NULL AND incident_zip IS NOT NULL;

            CREATE TEMP TABLE zip_unique_map AS
            SELECT incident_zip, ANY_VALUE(borough) AS borough
            FROM {self.table_name}
            WHERE incident_zip IN (SELECT incident_zip FROM null_zips)
            AND borough IS NOT NULL
            GROUP BY incident_zip
            HAVING COUNT(DISTINCT borough) = 1;

            CREATE OR REPLACE TABLE {self.table_name} AS
            SELECT
            n.* REPLACE (COALESCE(n.borough, z.borough) AS borough)
            FROM {self.table_name} n
            LEFT JOIN zip_unique_map z USING (incident_zip); 
                                
            DROP TABLE null_zips;
            DROP TABLE zip_unique_map;''')
    
    def drop_nulls(self, column_name: str):
        self.connection.execute(f'''
            DELETE FROM {self.table_name}
            WHERE {column_name} IS NULL;''')
    
    def standardize_complaint_column(self):
       self.connection.execute(f'''
            UPDATE {self.table_name}
            SET complaint_type = 
            TRIM(UPPER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(complaint_type), '[^a-zA-Z]', ' ', 'g'), '[[:space:]]+', ' ', 'g')));

            CREATE TEMP TABLE count_cutoff AS
            WITH complaint_type_counts AS (
            SELECT complaint_type, COUNT(*) AS count
            FROM nyc311
            GROUP BY complaint_type
            )
            SELECT QUANTILE_CONT(count, 0.25) AS count_25th_percentile
            FROM complaint_type_counts;

            CREATE TEMP TABLE complaint_counts AS
            SELECT complaint_type, COUNT(*) AS count_records FROM nyc311
                GROUP BY complaint_type 
                ORDER BY count_records DESC;

            CREATE OR REPLACE TABLE nyc311 AS
            SELECT * FROM nyc311 
            WHERE complaint_type in (
                SELECT DISTINCT complaint_type 
                FROM complaint_counts
                WHERE count_records > (
                    SELECT count_25th_percentile
                    FROM  count_cutoff
                ) 
            AND complaint_type NOT IN ('', ' ')
            );

            DROP TABLE count_cutoff;
            DROP TABLE complaint_counts;
            ''')
    def group_complaint_types(self):
        remap_complaints = {'ALZHEIMER S CARE':'ELDER CARE',
            'DAY CARE':'DAY CARE',
            'ELDER ABUSE':'ELDER CARE',
            'EVICTION':'EVICTION',
            'ILLEGAL FIREWORKS':'ILLEGAL FIREWORKS',
            'FACE COVERING VIOLATION':'FACE COVERING VIOLATION',
            'CITY VEHICLE PLACARD COMPLAINT':'NOISE',
            'NOISE HELICOPTER':'NOISE',
            'NOISE VEHICLE':'NOISE',
            'COLLECTION TRUCK NOISE	':'NOISE',
            'DEAD DYING TREE':'DAMAGED TREE',
            'DEAD TREE':'DAMAGED TREE',
            'DRINKING':'DRUG ACTIVITY',
            'DRINKING WATER':'DRINKING WATER',
            'MOSQUITOES':'MOSQUITOES',
            'UPROOTED STUMP':'DAMAGED TREE',
            'URINATING IN PUBLIC':'URINATING IN PUBLIC',
            'UNLEASHED DOG':'UNLEASHED DOG',
            'WATER CONSERVATION':'WATER CONSERVATION',
            'WATER LEAK':'WATER MAINTENANCE',
            'WATER MAINTENANCE':'WATER MAINTENANCE',
            'WATER QUALITY':'WATER MAINTENANCE',
            'WATER SYSTEM':'WATER MAINTENANCE',
            'MUNICIPAL PARKING FACILITY':'MUNICIPAL PARKING FACILITY',
            'PARKING CARD':'PARKING CARD',
            'VIOLATION OF PARK RULES':'IOLATION OF PARK RULES'}
        rename_broad_categories = {'ADOPT A BASKET':'LITTER BASKET',
            'AHV INSPECTION UNIT':'INSPECTIONS',
            'ANIMAL FACILITY NO PERMIT':'ANIMAL FACILITY',
            'BEST SITE SAFETY':'SITE SAFETY',
            'BLOCKED DRIVEWAY':'OBSTRUCTION',
            'BUILDING USE':'CONSTRUCTION',
            'CITY VEHICLE PLACARD COMPLAINT':'VEHICLE COMPLAINT',
            'COVID NON ESSENTIAL CONSTRUCTION':'COVID'}
    
        complaint_yaml = 'config/config.yaml'

        cwd = Path.cwd().parent 

        file_path = str(cwd) + "/" + complaint_yaml

        with open(file_path, "r") as f:
            config = yaml.safe_load(f)
        
        mapping = []
        for group, keys in config["complaint_groups"].items():
            for k in keys:
                mapping.append((k, group))
        final_mapping_df = pd.DataFrame(mapping, columns=['complaint_type_raw','complaint_type'])
        final_mapping_df['complaint_type']['complaint_type'] = final_mapping_df['complaint_type'].replace(rename_broad_categories)
        final_mapping_df["complaint_type"] = final_mapping_df["complaint_type_raw"].map(remap_complaints).fillna(final_mapping_df["complaint_type"])

        groups_dict = {}
        for group, group_df in final_mapping_df.groupby("complaint_type"):
            groups_dict[group] = sorted(group_df['complaint_type_raw'].unique().tolist())

        utils.save_yaml(groups_dict, 'config/config.yaml', 'complaint_groups')

        self.connection.register("complaints_normalized", final_mapping_df)
        self.connection.execute(f'''
            CREATE OR REPLACE TABLE {self.table_name} AS
            SELECT n.* REPLACE (c.complaint_type AS complaint_type), complaint_type_raw
            FROM {self.table_name} n
            LEFT JOIN complaints_normalized c ON n.complaint_type == c.complaint_type_raw;
            ''')
        self.connection.unregister("complaints_normalized")

    def join_with_population_data(self):
        self.connection.execute(f'''
            CREATE OR REPLACE TABLE {self.table_name} AS
            WITH complaints_with_pop_year AS (
                SELECT 
                    *,
                    CASE 
                        WHEN year BETWEEN 2010 AND 2014 THEN 2010
                        WHEN year BETWEEN 2015 AND 2025 THEN 2020
                    END AS year_range
                FROM nyc311
            )
            SELECT 
                c.*,
                p.population,
            FROM complaints_with_pop_year c
            LEFT JOIN borough_population p
                ON c.borough = p.borough
            AND c.year_range = p.year;
            ''')
    
    def create_building_info_table(self):
        building_info_df = utils.get_building_info()
        self.create_table_from_df(building_info_df, "building_info")


    def get_total_calls_by_borough(self):
        prop_total_by_borough = self.query_data_as_df('''
        WITH complaint_counts AS (
            SELECT 
                borough,
                COUNT(*) AS complaint_count
            FROM nyc311
            GROUP BY borough
        ),
        average_pop AS (
            SELECT borough, AVG(population) as average_population
            FROM borough_population
            GROUP BY borough
        )
        SELECT 
            c.borough,
            c.complaint_count,
            p.average_population,
            CAST(c.complaint_count AS DOUBLE) / p.average_population AS complaints_per_capita
        FROM complaint_counts c
        JOIN average_pop p
        ON UPPER(c.borough) = UPPER(p.borough)

        ORDER BY c.borough, complaints_per_capita DESC;

        ''')
        return prop_total_by_borough
    
    def get_top_complaint_by_borogh(self):
            top_comaplaints_by_borough = self.query_data_as_df('''
        WITH complaint_counts AS (
            SELECT 
                borough,
                complaint_type,
                COUNT(*) AS complaint_count
            FROM nyc311
            GROUP BY borough, complaint_type
        ),
        average_pop AS (
            SELECT borough, AVG(population) as average_population
            FROM borough_population
            GROUP BY borough
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY borough
                    ORDER BY complaint_count DESC
                ) AS rank_num
            FROM complaint_counts
        )
        SELECT 
            r.borough,
            r.complaint_type,
            r.complaint_count,
            p.average_population,
            CAST(r.complaint_count AS DOUBLE) / p.average_population AS complaints_per_capita
        FROM ranked r
        JOIN average_pop p
        ON UPPER(r.borough) = UPPER(p.borough)

        WHERE r.rank_num <= 2
        ORDER BY r.borough, complaints_per_capita DESC;

        ''')
            return top_comaplaints_by_borough
    
    def get_complaint_time_series(self):
        complaint_ts = self.query_data_as_df('''
        WITH complaint_counts AS (
            SELECT 
                borough,
                year,
                COUNT(*) AS complaint_count
            FROM nyc311
            GROUP BY borough, year
        ),
        average_pop AS (
            SELECT borough, AVG(population) as average_population
            FROM borough_population
            GROUP BY borough
        )
        SELECT 
            r.borough,
            r.year,
            r.complaint_count,
            p.average_population,
            CAST(r.complaint_count AS DOUBLE) / p.average_population AS complaints_per_capita
        FROM complaint_counts r
        JOIN average_pop p
        ON UPPER(r.borough) = UPPER(p.borough)

        ORDER BY r.borough, complaints_per_capita DESC;

        ''')
        return complaint_ts
    
    def get_bronx_timeseries(self):
        noise_ts_year_month_all = self.query_data_as_df('''
        WITH complaint_counts AS (
            SELECT 
                borough,
                DATE_TRUNC('day',created_date) as created_date_trunc,
                complaint_type,
                COUNT(*) AS complaint_count
            FROM nyc311
            GROUP BY created_date_trunc,borough,complaint_type
        ),
        average_pop AS (
            SELECT borough, AVG(population) as average_population
            FROM borough_population
            GROUP BY borough
        )
        SELECT 
            r.borough,
            r.created_date_trunc,
            r.complaint_count,
            p.average_population,
            CAST(r.complaint_count AS DOUBLE) / p.average_population AS complaints_per_capita
        FROM complaint_counts r
        JOIN average_pop p
        ON UPPER(r.borough) = UPPER(p.borough)
        WHERE r.borough = 'BRONX'
        AND r.complaint_type = 'NOISE'
        ORDER BY r.borough, complaints_per_capita DESC;

        ''')
        return noise_ts_year_month_all
    
    def get_bronx_ts_granular(self):
        noise_ts_granular = self.query_data_as_df('''
        WITH complaint_counts AS (
            SELECT 
                borough,
                DATE_TRUNC('hour',created_date) as created_date_trunc,
                complaint_type,
                COUNT(*) AS complaint_count
            FROM nyc311
            GROUP BY created_date_trunc,borough,complaint_type
        ),
        average_pop AS (
            SELECT borough, AVG(population) as average_population
            FROM borough_population
            GROUP BY borough
        )
        SELECT 
            r.borough,
            r.created_date_trunc,
            r.complaint_count,
            p.average_population,
            CAST(r.complaint_count AS DOUBLE) / p.average_population AS complaints_per_capita
        FROM complaint_counts r
        JOIN average_pop p
        ON UPPER(r.borough) = UPPER(p.borough)
        WHERE r.borough = 'BRONX'
        AND r.complaint_type = 'NOISE'
        ORDER BY r.borough, complaints_per_capita DESC;

        ''')
        return noise_ts_granular
