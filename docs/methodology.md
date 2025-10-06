## Methodological and implementation choices

### 1. Initial scope
I first examined the NYC 311 API to understand the overall size of data and what this data contains. I outlined how to connect this dataset with the overall prompt, focusing on high-level components such as:
- Performance speed (large database, slow API)
- Environment reproducibility 
- Real-world issues impacting 311 calls
- Good end user experience 

This ensured my approach considered both the technical and business impact.

### 2. Project design 
I utilized a modular framework with a class-based architecture. This allows for:
- **Cleaner presentation:** Limiting the code in the final project and notebooks for improved readability 
- **Separation of duties:** Each module handles well-defined task.  
- **Reusability:** The structure and methods allow for a general structure which can be reused for different types of complaints.


### 3. Technology choices

- **DuckDB:** This tool is a perfect choice for this project as it is lightweight, does not require external servers, performs well for mid-sized data, and integrates well with other packages.
    - An alternative would have been PySpark, but given the data size, DuckDB is sufficient and limits the additional setup needed for PySpark. 
    - DuckDB's syntax also lends itself to future maintainability
    - The model is run only on a subset of data, so DuckDB had no performance issues. If a model is fit on a larger portion of the dataset, PySpark would be a more suitable choice.
- **Poetry**
    - Poetry is a better choice than a requirements.txt, for example. 
    - It inherently incorporates a dependency solver, allows for virtual environments/kernels, and compatibility checks for breaking changes 
    - Using a virtual environment connected to a kernel ensures that anyone can reproduce the results of this project

### 4. Implementation Strategy
- **API efficiency:** The NYC 311 API is fairly large and slow, so I considered this in my implementation. I tested out different methods of calling the API (limit, paging, where clauses). Limit with simple paging was extremely slow. For this type of API, the performance is significantly improved when adding a where clause to the paging. In my code, I call the API using created_date. I limit to 50,000 rows with my API key so to not experience throttling. Asynchronous requests were also tested, but may also result in throttling. 
- **Data loading:** Because the API is not efficient, I chose to save the majority of the data to parquet files partitioned by borough. These parquet files are small (better for git) and connect very easily to DuckDB. The project is defaulted to only request the API for dates not existing in the parquets at the time this notebook is run, which improves the performance of the notebooks.
- **Classes and methods:** I chose to create classes for the API and database so that additional connections could be created without duplicating code. The methods are partially for readability as well, so the long queries/transformations don't impact the flow.
- **Configurations:** I chose to use a yaml configuration for the complaint types mapping. Yamls are very easy to read, are concise, support various data types, and are simple to modify. This allows for easy, quick mapping between the raw complaints and their groupings. 

### 4. Model choices
I considered both real-world applications and model accuracy when deciding on a framework. A complex model would not necessarily translate well to an organization considering staffing resources. It's important that the model, its inputs, and outputs are all interpretable by the user.
- **Scope:** Noise complaints in the Bronx
    - I intentionally wanted to model the complaint types with the highest call volume in the borough with the most calls. This is the first step in recommending where and how the city should staff resources. 
- **General model type:** Classification
    - I decided on a classification model to identify high complaint days rather than a model to predict number of complaints in the future. 
    - Providing a clear indicator on high complaint days is easier to understand than providing a number/proportion of complaints (what is the cutoff for providing more resources? how to separate overall increasing complaints from a surge in complaints?)
- **Classification type:** Logistic regression 
    - **Benefits:** coefficients are easily interpretable (log-odds) and performs efficiently on this size of data. 
    - I tested other classification models such as XGBoost, but the precision/recall/AUC and performance were similar since this data and features are straightforward (e.g. night --> more complaints).
    - For this use case, a logistic regression is a clearer model to explain to an audience planning for city resources because there are fewer parameters to tune and explain the choices of. This will aid in maintainability in the future. 
    - If expanding to a larger dataset with more complex features, XGBoost could be revisited.
- **Model choices:**
    - In order to improve model estimations, I found the optimal probability threshold when classifying as a high complaint day or normal day. 
    - This threshold is optimized by finding the highest precision and highest recall combination possible.
    - Modifying this threshold improves precision, recall, AUC, and TP/FPs. 
    - Response variable:
        - 1 = high complaint period, 0 = normal period 
        - High complaint period is defined as having complaint count in the 70th percentile. These percentiles are estimated each year because complaints have been increasing over recent years (either due to more calls or better data logging), so earlier periods would be over classified if using a percentile on the entire set. 
- **Features:** 
    - The goal in defining the features is to also keep interpretability in mind, otherwise it is difficult to say exactly when/where more resources should be staffed
    - I first visualized the patterns in the data and thought about what might be causing them (e.g. cyclical spikes, hotter months = more complaints?)
    - I then tested these hypotheses through slicing the data by  months, weekends, times of days etc. to confirm the patterns
    - I also calculated Z-scores to get and idea of what is being flagged as an anomaly to help inform features 
    - Combining encoded qualitative features (time of day, summer, weekend) with a quantitative feature (7-day lagged complaint counts) allow for different patterns in the data to be identified and modelled. The qualitative and quantitative features capture different elements of the data that just one type could not capture alone. 
- **Training and testing**
    - Since this dataset is a timeseries, I split up the data ordered by created_date (cannot do random shuffling with time series data as it will introduce data leakage)
    - 70/30 train/test split to ensure that there is a sufficient number of both classes in the sets
    - Trained on earlier data, predicted on recent data to replicate how this model would actually be used going forward 
- **Efficacy**
    - To measure efficacy, I considered the connection to the real-world problem being solved. Does this model clearly inform staffing concerns? How to translate this to an audience?
    - First, I checked the false/true positives with a confusion matrix and precision/recall. This shows how well the model identifies high complaint days, which is primarily what is needed for the city agency. 
    - In addition to knowing which days are surge days, it's important to distill when/where officers should be staffed because the city is very large and resources are limited. Examining the feature important helps to drive these decisions. Weekends and nights are by far the biggest drivers with the next most important being summer.
    - The model is considered to be effective for this use case as it is able to identify most high complaint days and clearly states at what times of days more resources are needed. 


### 7. Reflection and summary
- I focused on maintainability and readability, so there are opportunities to improve performance
    - Optimizing API requests 
    - Expanding to PySpark as the data grows 
    - Better storage of raw and cleaned datasets for more efficient reproducibility 
- I intentionally chose to focus on the Bronx and noise complaints.
    - This is the borough with the most calls and the type of complaint with the most calls. Because resourcing is limited, my goal was to zero in on an area/type with a particularly high issue. 
    - However, in future states, it would be interesting to see how these patterns persist in other boroughs and in other complaint types.  
- Model enhancements
    - Since noise complaint surges are relatively rare, the model could be biased towards the "normal complaint period" classification
    - We could introduce sample weighting in a future model to prevent bias and to identify even rarer surges to narrow down when to further increase resources 
