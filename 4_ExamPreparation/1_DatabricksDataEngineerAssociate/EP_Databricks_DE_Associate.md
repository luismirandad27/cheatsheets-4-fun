# Databricks Data Engineer Associate
#databricks #exampreparation #dataengineer

## Topic 1: *Understand how to use and the benefits of using the Databricks Lakehouse Platform and its tools*
- - - -
### 1. What is the *Databricks Lakehouse*?

- ACID transactions + data governance (from DWH).
- Enables BI + Machine Learning

#### <ins>Primary Components</ins>
- **Delta tables**
    - Databricks uses the Delta Lake protocol **by default**
    - **When a Delta Table is created**:
        - Metadata is added to the *metastore* (inside the declared schema or database).
        - *Data* and *table metadata* saved to a directory.
    - All Delta Tables **have**:
        - A *Directory* containing data in **Parquet**
        - A *Sub-directory* `/_delta_log` for metadata (table versions in **Json** and **Parquet**)
- **Metastore** (is optional)
    - We can interact with tables without metastore by using **Spark APIs**

Other topics related to *Delta Tables*:
- ACID Transactions
- Data Versioning
- ETL
- Indexing

#### <ins>Data Lakehouse vs Data Warehouse vs Data Lake</ins>
| Data Warehouse  | Data Lake  | Data Lakehouse  |
|---|---|---|
| Clean and structured data for BI Analytics  | Not for BI reporting due to its unvalidated nature  |  Low query latency and high reliability for BI/A |
|  Manage property formats | Stores data of any nature in any format  | Deals with many standard data formats  |
|   |   | ++ Indexis protocols optimized for ML and DS |

### 2. Data Science and Engineering Workspace

- For Data Analyst people -> you can use **Databricks SQL persona-based environment**

#### <ins>Workspace</ins>
- Organize objects like *notebooks*, *libraries*, *experiments*, *queries* and *dashboards*.
- Provides access to *data*
- Provides computational resources like *clusters* and *jobs*
- Can be managed by the **workspace UI** or **Databricks REST API reference**
- You can switch between workspaces.
- You can view the **new** databricks SQL queries, dashboards and alerts. **BUT**, to view the existing ones you need to **migrate** them into the wkspace browser

#### <ins>Workspace Assets</ins>

#### a. Clusters (Databricks Computer Resource)
- Provide unified platform for many use cases: *production ETL*, *pipelines*, *streaming analytics*, *ad-hoc analytics* and *ML*.
- **Cluster Types**:
    - <ins>*All-purpose clusters*</ins>: the user can manually terminate and restart them. **Multiple users** can share such clusters (*collaborative interactive analysis*)
    - <ins>*Job clusters*</ins>: dealed by the *Databricks job scheduler*
- Databricks can retain cluster config for *up to 200 all-purpose clusters* terminated in the last **30 days**, and *up to 30 job clusters* **recently terminated** (you can pin a cluster to the cluster list).

#### a.1. Create a Cluster
You can use the following options:
- Cluster UI
- Clusters API 2.0
    - Do not use hard code secrets or store them
    - Use the **Secrets API 2.0** to manage secrets in *Databricks CLI*.
    - Use the **Secrets utility** `dbutils.secrets` for referencing secrets in *notebooks* and *jobs*.
- HashiCorp Terraform

#### a.2. Manage a Cluster
* A cluster is permanently deleted after 30 days it’s terminated.
* To avoid this (for all-purpose clusters) the admin can **pin** the cluster (up to **100** can be pinned)
* **Edit** a Cluster:
	* Libraries installed remain after editing.
	* You must restart the cluster if you edit any attribute while it’s running.
	* Edito ONLY running or terminated clusters
* **Clone** a Cluster: cannot clone *permissions*, *installed libraries* and *attached notebooks*.
* **Control** access:
	* Cluster creation permission (admin)
	* Cluster-level permissions
* **Autostart** clusters:
	* When run a job related to a *terminated cluster*, the cluster is restarted.
* **Autotermination** clusters