# azure-databricks-snowflake-pipeline

AdventureWorks – End-to-End-Datenpipeline mit Azure Databricks und Snowflake
Prototyp einer mehrstufigen Datenpipeline für das fiktive Fahrradunternehmen AdventureWorks. Rohdaten aus Azure Data Lake Storage (ADLS) werden mit PySpark in Azure Databricks erfasst, bereinigt und inkrementell in Delta-Tabellen gespeichert. Anschließend werden sie in Snowflake geladen, dort zu einem Fakt-Dimensions-Modell (Star-Schema) verknüpft und für Reporting und Analytics aufbereitet.
Inhaltsverzeichnis
Projektziel
Architektur
Technologien
Datensatz
Projektstruktur
Pipeline im Detail
Schritt 1–2: Umgebung und Rohdaten
Schritt 3: Datenerfassung (standardized)
Schritt 4: Bereinigung und Transformation (processed)
Schritt 5: Laden nach Snowflake
Schritt 6: Fakt- und Dimensionstabellen
Schritt 7: Reporting und analytische Abfragen
Inkrementelle Ladelogik und Validierung
Ausführung
Sicherheitshinweise
Gelerntes
Projektziel
AdventureWorks kämpft mit verstreuten Datenquellen, wodurch das Reporting langsam und unzuverlässig ist. Dieses Projekt zeigt, wie eine hybride Datenarchitektur dieses Problem löst:
Databricks (Spark) übernimmt ETL: Erfassung, Bereinigung und Transformation großer Datenmengen.
Snowflake dient als analytisches Data Warehouse: SQL-basierte Abfragen ohne Cluster-Verwaltung.
Lernziele:
Aufbau einer mehrstufigen Datenpipeline (raw → standardized → processed → Snowflake)
ADLS in Databricks mounten und Daten lesen
Delta-Tabellen für historische und inkrementelle Daten erstellen
Daten mit PySpark bereinigen und transformieren
Inkrementelle Ladelogik (Append und Merge) implementieren und validieren
Snowflake einrichten, Daten per externem Stage laden und ein Star-Schema modellieren
Geschäftsfragen mit SQL beantworten
Architektur
        ROHDATENQUELLEN (ADLS: raw)
              CSV und JSON
                   │
                   ▼
        DATENERFASSUNG (standardized)
   Azure Databricks · Delta-Tabellen in ADLS
   Rohdaten + Metadaten, append-only, Historie
                   │
                   ▼
     BEREINIGUNG UND VERARBEITUNG (processed)
   Azure Databricks · Delta-Tabellen in ADLS
   bereinigt, typisiert, aktueller Stand (MERGE)
                   │
                   ▼
        SNOWFLAKE: RETAIL_SALES_DB
   STAGING  →  CORE (Star-Schema)  →  BI (Reports)
Technologien
Bereich	Technologie
Cloud-Speicher	Azure Data Lake Storage (ADLS)
ETL / Verarbeitung	Azure Databricks, PySpark
Tabellenformat	Delta Lake (Parquet + _delta_log)
Data Warehouse	Snowflake (Virtual Warehouse COMPUTE_WH)
Zugriff ADLS → Snowflake	Externer Stage mit SAS-Token, COPY INTO
Sprachen	Python (PySpark), SQL
Datensatz
AdventureWorks (Microsoft-Beispieldatenbank) mit Daten zu Kundschaft, Produkten, Bestellungen und Vertriebsgebieten. Eingesetzte Dateien:
Datei	Format
Customer.json	JSON (verschachtelt, mehrzeilig)
SalesOrderDetail.json	JSON (verschachtelt, mehrzeilig)
Person.csv	CSV
Address.csv	CSV
StateProvince.csv	CSV
CountryRegion.csv	CSV
Product.csv	CSV
ProductCategory.csv	CSV (mit Kopfzeile)
ProductSubCategory.csv	CSV (mit Kopfzeile)
SalesOrderHeader.csv	CSV
Projektstruktur
ADLS-Container
<container>/
├── raw/            # Originaldateien (CSV, JSON)
├── standardized/   # Delta-Tabellen, append-only, mit Metadaten
└── processed/      # Delta-Tabellen, bereinigt, per MERGE gepflegt
Snowflake
RETAIL_SALES_DB
├── RETAIL_SALES_STAGING   # Tabellen aus processed (COPY INTO)
├── RETAIL_SALES_CORE      # fact_sales_per_order_line, dim_product, dim_customer, dim_location
└── RETAIL_SALES_BI        # rep_monthly_sales, rep_regional_sales, rep_top_products, rep_top_customers
Dateien im Repository
Datei	Inhalt
01_pipeline_notebook.ipynb	Databricks-Notebook: Mount, Erfassung, Bereinigung, Merge, Validierung (Schritte 1–4)
SQL-Skript (Snowflake)	Stage, COPY INTO, Core-Modell, Reports und Abfragen (Schritte 5–7)
Pipeline im Detail
Schritt 1–2: Umgebung und Rohdaten
Databricks-Workspace in der Azure Sandbox, Notebook auf dem Shared Cluster.
In ADLS wird das Verzeichnis raw angelegt und die zehn Quelldateien werden hochgeladen.
Schritt 3: Datenerfassung (standardized)
Ziel: Daten so speichern, wie sie ankommen (Audit und Nachverarbeitung), ohne Bereinigung.
Mount: Der ADLS-Container wird mit dbutils.fs.mount() per SAS-Token eingebunden.
CSV-Dateien: Für jede Datei ist ein Schema als DDL-String definiert. Nur ProductCategory und ProductSubCategory haben eine Kopfzeile. Unsichere Spalten (z. B. Datumsfelder, IDs mit Dezimalstellen) werden bewusst als STRING bzw. DOUBLE gelesen, damit der Einlesevorgang nicht abbricht.
JSON-Dateien: Beide Dateien werden mit multiLine=true gelesen. Die verschachtelten Strukturen werden direkt beim Einlesen geflattet:
Customer.json: Felder aus customer und customer_details
SalesOrderDetail.json: Felder aus orderLine und product
Metadatenspalten: ingest_timestamp (current_timestamp()) und source_file (Dateipfad).
Append-Logik: Beim ersten Lauf werden alle Zeilen geschrieben. Bei späteren Läufen werden mit subtract nur Zeilen angehängt, die in der Tabelle noch nicht exakt so existieren (neue oder geänderte Datensätze). Es wird nie gelöscht oder überschrieben, die Historie bleibt erhalten.
Schritt 4: Bereinigung und Transformation (processed)
Aus den standardized-Tabellen entstehen bereinigte Tabellen mit dem aktuellen Stand der Geschäftsdaten.
Tabelle	Bereinigung
Person	Suffix &| aus ModifiedDate entfernt, Umwandlung in Timestamp
Address	SpatialLocation, rowguid entfernt; Datumstypen korrigiert
CountryRegion	leere Hilfsspalte _c3 entfernt; Datumstypen korrigiert
StateProvince	rowguid entfernt; Datumstypen korrigiert
Product	MakeFlag, FinishedGoodsFlag, DaysToManufacture, rowguid entfernt; ProductSubCategoryID und ProductModelID von String über Double zu Integer; Datumsfelder als Timestamp
ProductCategory / SubCategory	rowguid entfernt; Datumstypen korrigiert
SalesOrderHeader	Status-Codes 1–6 in Text (In process, Approved, Backordered, Rejected, Shipped, Cancelled; sonst Unknown); rowguid, Comment entfernt; Datums- und Integer-Typen korrigiert
Customer	AW durch CustNo in den Kontonummern ersetzt; rowguid entfernt; Datumstyp korrigiert
SalesOrderDetail	negative unitPrice-Werte per Betrag korrigiert; fehlende orderQty aus lineTotal / (unitPrice * (1 - unitPriceDiscount)) berechnet; rowGuid entfernt; Datentypen korrigiert
Inkrementelles Laden (MERGE):
Da standardized mehrere Versionen pro Datensatz enthält, wird pro Schlüssel mit row_number() (Sortierung nach ModifiedDate und ingest_timestamp absteigend) nur die neueste Version behalten.
Erster Lauf: Tabelle wird komplett nach processed geschrieben.
Folgeläufe: MERGE über die Merge-Schlüssel. Bestehende Zeilen werden nur aktualisiert, wenn s.ModifiedDate > t.ModifiedDate; neue Schlüssel werden eingefügt.
Tabelle	Merge-Schlüssel
person	BusinessEntityID
address	AddressID
countryregion	CountryRegionCode
stateprovince	StateProvinceID
product	ProductID
productcategory	ProductCategoryID
productsubcategory	ProductSubCategoryID
salesorderheader	SalesOrderID
customer	customerId
salesorderdetail	salesOrderId, salesOrderDetailId
Schritt 5: Laden nach Snowflake
Virtual Warehouse (COMPUTE_WH), Datenbank RETAIL_SALES_DB und Schema RETAIL_SALES_STAGING anlegen.
SAS-Token im Azure Portal erzeugen (User delegation key, Berechtigungen Read und List).
Externen Stage adls_stage anlegen, der auf processed/ zeigt, und mit LIST @adls_stage; prüfen.
Pro Tabelle: CREATE TABLE mit passenden Typen und COPY INTO aus den Parquet-Dateien (FILE_FORMAT = Parquet, MATCH_BY_COLUMN_NAME = case_insensitive, PATTERN je Tabellenordner).
Zeilenanzahlen aller zehn Tabellen werden zur Kontrolle abgefragt.
Staging-Tabellen: CUSTOMER, PERSON, ADDRESS, STATE_PROVINCE, COUNTRY_REGION, PRODUCT, PRODUCT_CATEGORY, PRODUCT_SUBCATEGORY, SALES_ORDER_HEADER, SALES_ORDER_DETAIL.
Schritt 6: Fakt- und Dimensionstabellen
Schema RETAIL_SALES_CORE:
Tabelle	Quellen	Join	Granularität
fact_sales_per_order_line	SalesOrderDetail + SalesOrderHeader	INNER JOIN auf SalesOrderID	eine Zeile pro Bestellposition
dim_product	Product + ProductSubCategory + ProductCategory	LEFT JOIN	eine Zeile pro Produkt
dim_customer	Customer + Person	LEFT JOIN (nicht jede Kundschaft hat eine Person)	eine Zeile pro Kunde
dim_location	Address + StateProvince + CountryRegion	LEFT JOIN	eine Zeile pro Adresse
Spalten werden in sprechende Namen umbenannt (z. B. unit_price_usd, order_status, modified_at), Datumsfelder der Faktentabelle werden zu DATE umgewandelt. In dim_customer ist store_id ein NULL-Platzhalter, da die Quelldaten keine StoreID enthalten.
Qualitätskontrollen: Zeilenanzahl der Faktentabelle gegen SALES_ORDER_DETAIL, Dimensionen gegen ihre Quelltabellen sowie Prüfung, dass keine Faktenzeilen ohne passendes Produkt oder passende Kundschaft existieren (erwartet: 0).
Schritt 7: Reporting und analytische Abfragen
Schema RETAIL_SALES_BI. Alle Reports berücksichtigen nur Bestellungen mit Status Shipped.
Tabelle	Inhalt	Besonderheit
rep_monthly_sales	eine Zeile pro Monat: Bestellungen, Umsatz, Menge, Steuer, Fracht	Steuer und Fracht stehen im Header und werden pro Bestellung wiederholt; deshalb Aggregation auf Bestellebene (SELECT DISTINCT), getrennt von der Positionsebene, um Doppelzählungen zu vermeiden
rep_regional_sales	eine Zeile pro Territory: Land, Bundesstaaten/Provinzen, Bestellungen, Umsatz, Menge	Mehrere Bundesstaaten pro Territory werden mit LISTAGG zusammengefasst, damit Umsätze nicht mehrfach gezählt werden
rep_top_products	eine Zeile pro Produkt: Umsatz, Bestellungen, Menge	–
rep_top_customers	eine Zeile pro Kunde: Umsatz	Basis für Top-10-Abfrage
Beantwortete Geschäftsfragen:
Top-10-Kundschaft nach Umsatz
Top-3-Produkte nach verkaufter Menge
Top-5-Produkte nach Umsatz
Jede Reporting-Tabelle wird kontrolliert, indem die Umsatzsumme mit der Summe der Faktentabelle (order_status = 'Shipped') verglichen wird.
Inkrementelle Ladelogik und Validierung
standardized (Append), Test mit neuen Produktdaten:
Product.csv in raw löschen und ProductUpdated.csv als Product.csv hochladen (enthält die neuen Datensätze mit ProductID 1000 und 1001).
Rohdaten-Einlesen und Append-Logik erneut ausführen.
Prüfung: Zeilenanzahl vorher/nachher per Time Travel (versionAsOf), Filter auf ProductID 1000 und 1001 sowie Gruppierung nach ingest_timestamp und source_file. Die alten Datensätze bleiben erhalten, die neuen werden angehängt.
processed (MERGE), Test mit StateProvince:
Zeilen mit Name = "Seine (Paris)" anzeigen und zählen.
Neue Version dieser Zeilen an standardized anhängen (Name = "Paris", StateProvinceCode = "FR-PAR", ModifiedDate = current_timestamp()).
Bereinigungs- und Merge-Zellen erneut ausführen.
Prüfung in processed/stateprovince: Es gibt keine Zeile „Seine (Paris)“ mehr, die Zeile „Paris“ hat dieselbe StateProvinceID und die aktualisierten Werte. Damit funktioniert die inkrementelle Logik.
Ausführung
Voraussetzungen
Azure-Sandbox-Konto mit Zugriff auf Databricks und ADLS
Snowflake-Konto (z. B. Testversion)
Quelldateien im Verzeichnis raw des ADLS-Containers
Reihenfolge
Notebook 01_pipeline_notebook.ipynb in Databricks importieren und mit dem Shared Cluster verbinden.
Im ersten Block sas_token, container_name und storage_account_name eintragen und den Container mounten.
Zellen von Schritt 3 (Schemas, Einlesen, Append-Logik) ausführen.
Zellen von Schritt 4 (Bereinigung, Merge-Schlüssel, Merge) ausführen.
In Snowflake das SQL-Skript abschnittsweise ausführen: Stage und COPY INTO (Schritt 5), Core-Modell (Schritt 6), Reports und Abfragen (Schritt 7). Dabei SAS-Token, Storage-Account und Container im Stage-Statement anpassen.
Nach Abschluss der Bereinigung den Mount mit dbutils.fs.unmount() wieder entfernen.
Sicherheitshinweise
Der Databricks-Workspace ist eine gemeinsam genutzte Umgebung. Notebooks und Dateien sind nicht privat.
Keine SAS-Tokens, Zugriffsschlüssel oder Zugangsdaten im Notebook, im SQL-Skript oder im Repository speichern. In diesem Repository sind alle Zugangsdaten durch Platzhalter ersetzt.
SAS-Tokens nur mit minimalen Rechten (Read, List) und kurzer Gültigkeit erzeugen.
Notebook regelmäßig lokal sichern.
Gelerntes
Eine mehrstufige Architektur (raw, standardized, processed) trennt Rohdatenhaltung, Historie und bereinigten aktuellen Stand sauber voneinander.
Append-only in der standardized-Schicht erhält die Historie; MERGE mit ModifiedDate-Bedingung in der processed-Schicht verhindert unnötige Updates.
Bei Joins zwischen Header und Detail (z. B. Steuer, Fracht) und bei Dimensionen mit mehreren Ausprägungen (Territory mit mehreren Bundesstaaten) muss die Granularität beachtet werden, sonst entstehen Doppelzählungen.
Kontrollabfragen (Zeilenanzahlen, Summenvergleiche zwischen Fakt und Report) sind wichtig, um die Korrektheit des Datenmodells zu belegen.
